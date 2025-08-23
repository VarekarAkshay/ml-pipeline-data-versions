import subprocess
import logging
import pathlib
import json
from datetime import datetime
import sys
import yaml
import os

class DataVersioningManager:
    def __init__(self, logger, base_folder, config, config_file_path):
        self.logger = logger
        self.base_folder = pathlib.Path(base_folder).resolve()
        self.config = config
        self.config_dir = pathlib.Path(config_file_path).parent.resolve()

        # Git root is the parent folder (project root)
        self.git_path = self.base_folder

        # Resolve metadata and logs paths relative to project root
        self.metadata_path = self.base_folder / self.config['metadata']['metadata_dir']
        self.logs_path = self.base_folder / self.config['output']['logs_dir']

        for dir_path in [self.metadata_path, self.logs_path]:
            dir_path.mkdir(parents=True, exist_ok=True)

        self.current_version = self.config.get('versioning', {}).get('initial_version', '0.1.0')

    def resolve_data_source_path(self, relative_path):
        """
        Resolve data source paths relative to config file location.
        """
        if relative_path.startswith('../'):
            # Path is relative to config file location (08_Data_Versioning)
            resolved = (self.config_dir / relative_path).resolve()
        else:
            # Path is relative to project root
            resolved = (self.base_folder / relative_path).resolve()
        return resolved

    def save_metadata(self, metadata_file, version_data):
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(version_data, f, indent=2, ensure_ascii=False)
        self.logger.info(f"Saved version metadata to: {metadata_file}")

    def stage_data_sources(self):
        """
        Stage only files inside configured raw and transformed dataset folders.
        """
        for key, source in self.config.get('data_sources', {}).items():
            # Resolve path correctly relative to config file location
            data_dir = self.resolve_data_source_path(source['path'])
            
            if not data_dir.exists():
                self.logger.warning(f"Data source path not found, skipping: {data_dir}")
                continue
            
            # Get relative path from git root for staging
            try:
                rel_path = os.path.relpath(str(data_dir), str(self.git_path))
                self.logger.info(f"Staging data source: {key} at {rel_path}")
                subprocess.run(['git', 'add', rel_path], cwd=self.git_path, check=True)
            except ValueError:
                self.logger.error(f"Cannot create relative path for {data_dir} from {self.git_path}")
                continue

    def git_commit(self, message):
        status_check = subprocess.run(
            ['git', 'status', '--porcelain'],
            cwd=self.git_path,
            capture_output=True,
            text=True
        )
        if status_check.stdout.strip():
            subprocess.run(['git', 'commit', '-m', message], cwd=self.git_path, check=True)
            self.logger.info(f"Git commit created: {message}")
        else:
            self.logger.info("No changes detected; skipping git commit.")

    def run_versioning(self, description=""):
        try:
            self.logger.info("Starting data versioning...")

            # Create metadata
            metadata_file = self.metadata_path / f"version_{self.current_version}.json"
            version_metadata = {
                "version": self.current_version,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "description": description,
                "tracked_data_sources": {
                    k: str(self.resolve_data_source_path(v['path'])) 
                    for k, v in self.config.get('data_sources', {}).items()
                }
            }

            self.save_metadata(metadata_file, version_metadata)

            # Stage data sources
            self.stage_data_sources()

            # Add metadata file
            rel_meta = os.path.relpath(str(metadata_file), str(self.git_path))
            subprocess.run(['git', 'add', rel_meta], cwd=self.git_path, check=True)

            commit_msg = f"Version {self.current_version}: {description}"
            self.git_commit(commit_msg)

            self.logger.info("Data versioning completed successfully.")
            
        except Exception as e:
            self.logger.error(f"Data versioning failed: {e}")
            raise

def main():
    base_folder = pathlib.Path(os.getcwd()).resolve()
    config_path = base_folder / '08_Data_Versioning' / 'config_step8.yaml'

    # Create logs directory
    logs_dir = base_folder / '08_Data_Versioning' / 'logs'
    logs_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(logs_dir / 'data_versioning.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger = logging.getLogger("DataVersioning")

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logger.info(f"Loaded config file: {config_path}")

        # Pass config file path to manager for correct path resolution
        manager = DataVersioningManager(logger, base_folder, config, config_path)

        desc = "Initial version"
        if '--description' in sys.argv:
            idx = sys.argv.index('--description')
            if idx + 1 < len(sys.argv):
                desc = sys.argv[idx + 1]

        manager.run_versioning(desc)
        
    except Exception as err:
        logger.error(f"Execution failed: {err}")
        sys.exit(1)

if __name__ == "__main__":
    main()
