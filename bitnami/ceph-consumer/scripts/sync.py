#!/usr/bin/env python3
# Copyright The Rook Authors.
# SPDX-License-Identifier: Apache-2.0

"""
Ceph Consumer Sync Controller

Synchronizes Ceph storage resources from a provider cluster to a consumer cluster:
- Mon endpoints and FSID
- CSI user secrets (directly copied from provider)
- StorageClasses (directly copied, only namespace references replaced)

Designed to run as a CronJob for periodic full reconciliation.
"""

import argparse
import base64
import copy
import json
import logging
import os
import re
import subprocess
import sys
from typing import Any, Dict, List, Optional, Set

import yaml

# Configure logging
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class KubeClient:
    """Kubernetes client wrapper using kubectl with JSON I/O"""

    # Fields to remove from metadata when copying resources
    METADATA_FIELDS_TO_REMOVE = [
        "uid",
        "resourceVersion", 
        "creationTimestamp",
        "generation",
        "selfLink",
        "managedFields",
    ]

    def __init__(self, kubeconfig: Optional[str] = None):
        self.kubeconfig = kubeconfig
        self._kubectl_base = ["kubectl"]
        if kubeconfig:
            self._kubectl_base.extend(["--kubeconfig", kubeconfig])

    def _run(
        self, args: List[str], input_data: Optional[str] = None, check: bool = True
    ) -> subprocess.CompletedProcess:
        """Run kubectl command"""
        cmd = self._kubectl_base + args
        logger.debug(f"Running: {' '.join(cmd)}")
        
        result = subprocess.run(
            cmd,
            input=input_data,
            capture_output=True,
            text=True,
        )
        
        if check and result.returncode != 0:
            logger.error(f"Command failed: {result.stderr}")
            raise RuntimeError(f"kubectl command failed: {result.stderr}")
        
        return result

    def get(
        self, resource: str, name: str = "", namespace: str = ""
    ) -> Optional[Dict[str, Any]]:
        """Get a Kubernetes resource as JSON"""
        args = ["get", resource, "-o", "json"]
        if name:
            args.insert(2, name)
        if namespace:
            args.extend(["-n", namespace])

        result = self._run(args, check=False)
        if result.returncode != 0:
            logger.debug(f"Resource not found: {resource}/{name}")
            return None

        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
            return None

    def get_list(
        self, resource: str, namespace: str = "", label_selector: str = ""
    ) -> List[Dict[str, Any]]:
        """Get list of Kubernetes resources"""
        args = ["get", resource, "-o", "json"]
        if namespace:
            args.extend(["-n", namespace])
        if label_selector:
            args.extend(["-l", label_selector])

        result = self._run(args, check=False)
        if result.returncode != 0:
            logger.warning(f"Failed to list {resource}: {result.stderr}")
            return []

        try:
            data = json.loads(result.stdout)
            return data.get("items", [])
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
            return []

    def apply(self, manifest: Dict[str, Any]) -> bool:
        """Apply a Kubernetes manifest using JSON"""
        args = ["apply", "-f", "-"]
        json_data = json.dumps(manifest)
        
        result = self._run(args, input_data=json_data, check=False)
        
        if result.returncode != 0:
            logger.error(f"Failed to apply {manifest.get('kind')}/{manifest.get('metadata', {}).get('name')}: {result.stderr}")
            return False
        
        logger.info(f"Applied {manifest['kind']}/{manifest['metadata']['name']}")
        return True

    def delete(
        self, resource: str, name: str, namespace: str = "", ignore_not_found: bool = True
    ) -> bool:
        """Delete a Kubernetes resource"""
        args = ["delete", resource, name]
        if namespace:
            args.extend(["-n", namespace])
        if ignore_not_found:
            args.append("--ignore-not-found")

        result = self._run(args, check=False)
        if result.returncode == 0:
            logger.info(f"Deleted {resource}/{name}")
            return True
        return False

    @classmethod
    def clean_metadata(cls, metadata: Dict[str, Any], keep_annotations: bool = False) -> Dict[str, Any]:
        """Remove cluster-specific fields from metadata"""
        cleaned = {}
        
        for key, value in metadata.items():
            if key in cls.METADATA_FIELDS_TO_REMOVE:
                continue
            if key == "annotations" and not keep_annotations:
                # Remove kubectl annotation, keep others
                if value:
                    cleaned_annotations = {
                        k: v for k, v in value.items()
                        if not k.startswith("kubectl.kubernetes.io/")
                    }
                    if cleaned_annotations:
                        cleaned["annotations"] = cleaned_annotations
                continue
            if key == "ownerReferences":
                continue
            if key == "finalizers":
                continue
            cleaned[key] = value
        
        return cleaned


class CephConsumerSync:
    """Main sync controller class"""

    # CSI secret names in provider cluster
    CSI_SECRETS = [
        "rook-csi-rbd-node",
        "rook-csi-rbd-provisioner", 
        "rook-csi-cephfs-node",
        "rook-csi-cephfs-provisioner",
    ]

    def __init__(self, config_path: str, provider_kubeconfig: str):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        self.provider_client = KubeClient(provider_kubeconfig)
        self.consumer_client = KubeClient()  # Uses in-cluster config

        self.provider_ns = self.config["provider"]["namespace"]
        self.consumer_ns = self.config["consumer"]["namespace"]
        self.managed_labels = self.config["managedLabels"]

    # =========================================================================
    # Mon Endpoints Sync
    # =========================================================================

    def get_provider_mon_data(self) -> Optional[Dict[str, Any]]:
        """Get Mon endpoints and FSID from provider cluster"""
        # Get mon endpoints from ConfigMap
        cm = self.provider_client.get("configmap", "rook-ceph-mon-endpoints", self.provider_ns)
        if not cm:
            logger.error("Could not find rook-ceph-mon-endpoints ConfigMap")
            return None

        # Get FSID from Secret
        secret = self.provider_client.get("secret", "rook-ceph-mon", self.provider_ns)
        if not secret:
            logger.error("Could not find rook-ceph-mon Secret")
            return None

        cm_data = cm.get("data", {})
        secret_data = secret.get("data", {})

        # Decode FSID
        fsid_b64 = secret_data.get("fsid")
        if not fsid_b64:
            logger.error("FSID not found in rook-ceph-mon secret")
            return None

        try:
            fsid = base64.b64decode(fsid_b64).decode("utf-8")
        except Exception as e:
            logger.error(f"Failed to decode FSID: {e}")
            return None

        # Get mon data string: "a=10.32.16.10:3300,b=10.32.16.11:3300,..."
        mon_data = cm_data.get("data", "")
        if not mon_data:
            logger.error("Mon data not found in ConfigMap")
            return None

        # Get maxMonId
        max_mon_id = cm_data.get("maxMonId", "0")

        # Decode all secret fields for copying to consumer
        decoded_secrets = {}
        for key, value in secret_data.items():
            try:
                decoded_secrets[key] = base64.b64decode(value).decode("utf-8")
            except Exception as e:
                logger.warning(f"Failed to decode secret key {key}: {e}")

        logger.info(f"Provider FSID: {fsid}")
        logger.info(f"Provider Mon data: {mon_data}")

        return {
            "fsid": fsid,
            "mon_data": mon_data,
            "max_mon_id": max_mon_id,
            "secret_data": decoded_secrets,
        }

    def sync_mon_endpoints(self, mon_info: Dict[str, Any]) -> bool:
        """Sync Mon endpoints to consumer cluster"""
        success = True

        # Create/update ConfigMap
        configmap = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "rook-ceph-mon-endpoints",
                "namespace": self.consumer_ns,
                "labels": copy.deepcopy(self.managed_labels),
            },
            "data": {
                "data": mon_info["mon_data"],
                "mapping": "{}",
                "maxMonId": mon_info["max_mon_id"],
            },
        }

        if not self.consumer_client.apply(configmap):
            success = False

        # Build secret data from provider
        # Copy all fields from provider secret, add cluster-name
        secret_string_data = copy.deepcopy(mon_info.get("secret_data", {}))
        secret_string_data["cluster-name"] = self.consumer_ns
        
        # Ensure required fields exist (for external cluster compatibility)
        # Provider uses: ceph-secret, ceph-username, fsid, mon-secret
        # External cluster expects: admin-secret (or ceph-secret), fsid, mon-secret, cluster-name
        
        # Map ceph-secret to admin-secret if needed (for Rook external cluster compatibility)
        if "ceph-secret" in secret_string_data and "admin-secret" not in secret_string_data:
            secret_string_data["admin-secret"] = secret_string_data["ceph-secret"]
        
        # Create/update Mon secret
        secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "rook-ceph-mon",
                "namespace": self.consumer_ns,
                "labels": copy.deepcopy(self.managed_labels),
            },
            "type": "kubernetes.io/rook",
            "stringData": secret_string_data,
        }

        if not self.consumer_client.apply(secret):
            success = False

        return success

    # =========================================================================
    # CSI Secrets Sync
    # =========================================================================

    def sync_csi_secrets(self) -> bool:
        """Sync CSI secrets from provider to consumer cluster"""
        success = True

        for secret_name in self.CSI_SECRETS:
            provider_secret = self.provider_client.get("secret", secret_name, self.provider_ns)
            
            if not provider_secret:
                logger.warning(f"Secret {secret_name} not found in provider cluster")
                continue

            # Transform secret for consumer cluster
            consumer_secret = self.transform_secret(provider_secret, secret_name)
            
            if not self.consumer_client.apply(consumer_secret):
                success = False

        return success

    def transform_secret(self, provider_secret: Dict[str, Any], name: str) -> Dict[str, Any]:
        """Transform a secret from provider to consumer format"""
        # Deep copy and clean
        secret = {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": name,
                "namespace": self.consumer_ns,
                "labels": copy.deepcopy(self.managed_labels),
            },
            "type": provider_secret.get("type", "Opaque"),
        }

        # Copy data as-is (already base64 encoded)
        if "data" in provider_secret:
            secret["data"] = copy.deepcopy(provider_secret["data"])

        return secret

    # =========================================================================
    # StorageClass Sync  
    # =========================================================================

    def get_provider_storage_classes(self) -> List[Dict[str, Any]]:
        """Get Ceph CSI StorageClasses from provider cluster"""
        all_scs = self.provider_client.get_list("storageclasses")
        
        ceph_scs = []
        for sc in all_scs:
            provisioner = sc.get("provisioner", "")
            # Only process rook-ceph CSI provisioners
            if ".rbd.csi.ceph.com" in provisioner or ".cephfs.csi.ceph.com" in provisioner:
                ceph_scs.append(sc)
        
        logger.info(f"Found {len(ceph_scs)} Ceph StorageClasses in provider cluster")
        return ceph_scs

    def should_sync_storage_class(self, sc_name: str, provisioner: str, labels: Dict[str, str]) -> bool:
        """Check if StorageClass should be synced"""
        # Check exclude label first
        exclude_label = self.config["storageClassSync"]["filter"].get("excludeLabel", "ceph-consumer.rook.io/exclude")
        if labels.get(exclude_label) == "true":
            logger.debug(f"StorageClass {sc_name} excluded by label {exclude_label}=true")
            return False

        # Check RBD/CephFS enable flags
        if ".rbd.csi.ceph.com" in provisioner:
            if not self.config["storageClassSync"]["rbd"]["enabled"]:
                logger.debug(f"Skipping RBD StorageClass {sc_name} - RBD sync disabled")
                return False
        elif ".cephfs.csi.ceph.com" in provisioner:
            if not self.config["storageClassSync"]["cephfs"]["enabled"]:
                logger.debug(f"Skipping CephFS StorageClass {sc_name} - CephFS sync disabled")
                return False

        # Check filter patterns
        filter_config = self.config["storageClassSync"]["filter"]
        
        # Check exclude patterns
        for pattern in filter_config.get("excludePatterns", []):
            try:
                if re.match(pattern, sc_name):
                    logger.debug(f"StorageClass {sc_name} excluded by pattern {pattern}")
                    return False
            except re.error as e:
                logger.warning(f"Invalid regex pattern '{pattern}': {e}")

        # Check include patterns (if specified)
        include_patterns = filter_config.get("includePatterns", [])
        if include_patterns:
            for pattern in include_patterns:
                try:
                    if re.match(pattern, sc_name):
                        return True
                except re.error as e:
                    logger.warning(f"Invalid regex pattern '{pattern}': {e}")
            return False

        return True

    def transform_storage_class(self, provider_sc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform provider StorageClass for consumer cluster.
        
        Preserves all fields except:
        - metadata: cleaned of cluster-specific fields, adds managed labels
        - provisioner: namespace prefix replaced
        - parameters: clusterID and *-secret-namespace replaced
        """
        sc_name = provider_sc["metadata"]["name"]
        
        # Start with a clean copy
        sc = {
            "apiVersion": provider_sc.get("apiVersion", "storage.k8s.io/v1"),
            "kind": "StorageClass",
        }

        # Clean and transform metadata
        old_metadata = provider_sc.get("metadata", {})
        new_metadata = {
            "name": sc_name,
            "labels": copy.deepcopy(self.managed_labels),
        }

        # Preserve existing labels (except kubectl ones)
        old_labels = old_metadata.get("labels", {})
        for key, value in old_labels.items():
            if not key.startswith("kubectl.kubernetes.io/"):
                new_metadata["labels"][key] = value

        # Handle default StorageClass annotation
        old_annotations = old_metadata.get("annotations", {})
        config_default = self.config["storageClassSync"].get("defaultStorageClass", "")
        
        if config_default == sc_name:
            # Explicitly set as default
            new_metadata["annotations"] = {
                "storageclass.kubernetes.io/is-default-class": "true"
            }
        elif config_default == "" and old_annotations.get("storageclass.kubernetes.io/is-default-class") == "true":
            # Preserve provider's default if no override
            new_metadata["annotations"] = {
                "storageclass.kubernetes.io/is-default-class": "true"
            }

        sc["metadata"] = new_metadata

        # Transform provisioner: replace namespace prefix
        provisioner = provider_sc.get("provisioner", "")
        if ".rbd.csi.ceph.com" in provisioner:
            sc["provisioner"] = f"{self.consumer_ns}.rbd.csi.ceph.com"
        elif ".cephfs.csi.ceph.com" in provisioner:
            sc["provisioner"] = f"{self.consumer_ns}.cephfs.csi.ceph.com"
        else:
            sc["provisioner"] = provisioner

        # Transform parameters
        old_params = provider_sc.get("parameters", {})
        new_params = {}
        
        for key, value in old_params.items():
            if key == "clusterID":
                new_params[key] = self.consumer_ns
            elif key.endswith("-secret-namespace"):
                new_params[key] = self.consumer_ns
            else:
                new_params[key] = value

        sc["parameters"] = new_params

        # Copy other fields as-is
        for field in ["allowVolumeExpansion", "reclaimPolicy", "volumeBindingMode", "mountOptions"]:
            if field in provider_sc:
                sc[field] = provider_sc[field]

        return sc

    def sync_storage_classes(self) -> bool:
        """Sync StorageClasses from provider to consumer cluster"""
        if not self.config["storageClassSync"]["enabled"]:
            logger.info("StorageClass sync is disabled")
            return True

        success = True
        synced_scs: Set[str] = set()

        # Get all Ceph StorageClasses from provider
        provider_scs = self.get_provider_storage_classes()

        for provider_sc in provider_scs:
            sc_name = provider_sc["metadata"]["name"]
            provisioner = provider_sc.get("provisioner", "")
            labels = provider_sc.get("metadata", {}).get("labels", {})
            
            # Check if should sync
            if not self.should_sync_storage_class(sc_name, provisioner, labels):
                continue
            
            # Transform and apply
            consumer_sc = self.transform_storage_class(provider_sc)
            
            if self.consumer_client.apply(consumer_sc):
                synced_scs.add(sc_name)
            else:
                logger.error(f"Failed to sync StorageClass {sc_name}")
                success = False

        logger.info(f"Synced {len(synced_scs)} StorageClasses")

        # Clean up orphaned StorageClasses
        self.cleanup_orphaned_storage_classes(synced_scs)

        return success

    def cleanup_orphaned_storage_classes(self, valid_scs: Set[str]) -> None:
        """Delete StorageClasses that are managed by us but no longer exist in provider"""
        label_selector = "ceph-consumer.rook.io/managed=true"
        existing_scs = self.consumer_client.get_list("storageclasses", label_selector=label_selector)

        for sc in existing_scs:
            sc_name = sc["metadata"]["name"]
            if sc_name not in valid_scs:
                logger.info(f"Deleting orphaned StorageClass: {sc_name}")
                self.consumer_client.delete("storageclass", sc_name)

    # =========================================================================
    # Main Run
    # =========================================================================

    def run(self) -> bool:
        """Run the sync process"""
        logger.info("=" * 60)
        logger.info("Starting Ceph consumer sync")
        logger.info(f"Provider namespace: {self.provider_ns}")
        logger.info(f"Consumer namespace: {self.consumer_ns}")
        logger.info("=" * 60)

        overall_success = True

        # Step 1: Sync Mon endpoints and FSID
        logger.info("")
        logger.info("Step 1: Syncing Mon endpoints and FSID...")
        mon_info = self.get_provider_mon_data()
        if not mon_info:
            logger.error("Failed to get Mon data from provider")
            return False

        if not self.sync_mon_endpoints(mon_info):
            logger.error("Failed to sync Mon endpoints")
            overall_success = False

        # Step 2: Sync CSI secrets
        logger.info("")
        logger.info("Step 2: Syncing CSI secrets...")
        if not self.sync_csi_secrets():
            logger.warning("Some CSI secrets failed to sync")
            overall_success = False

        # Step 3: Sync StorageClasses
        logger.info("")
        logger.info("Step 3: Syncing StorageClasses...")
        if not self.sync_storage_classes():
            logger.warning("Some StorageClasses failed to sync")
            overall_success = False

        # Summary
        logger.info("")
        logger.info("=" * 60)
        if overall_success:
            logger.info("Sync completed successfully")
        else:
            logger.warning("Sync completed with some errors")
        logger.info("=" * 60)

        return overall_success


def main():
    parser = argparse.ArgumentParser(
        description="Sync Ceph resources from provider to consumer cluster"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to configuration file",
    )
    parser.add_argument(
        "--provider-kubeconfig",
        required=True,
        help="Path to provider cluster kubeconfig",
    )

    args = parser.parse_args()

    # Validate files exist
    if not os.path.isfile(args.config):
        logger.error(f"Config file not found: {args.config}")
        sys.exit(1)
    
    if not os.path.isfile(args.provider_kubeconfig):
        logger.error(f"Provider kubeconfig not found: {args.provider_kubeconfig}")
        sys.exit(1)

    syncer = CephConsumerSync(args.config, args.provider_kubeconfig)
    
    try:
        success = syncer.run()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.exception(f"Sync failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
