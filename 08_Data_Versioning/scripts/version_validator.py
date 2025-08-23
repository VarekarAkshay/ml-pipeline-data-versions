import os
import sys
import pathlib
import logging
import yaml
import json
import hashlib
import subprocess
from datetime import datetime
from typing import Dict, List, Any, Optional
import argparse

class VersionValidator:
    def __init__(self, logger, script_folder, config):
        self.logger = logger
        self.script_folder = script_folder
        self.config = config
        self.base_folder = script_folder.parent.resolve()
        
        # Initialize paths
        self.git_path = self.base_folder / self.config['version_control']['git']['repository_path']
        self.dvc_path = self.base_folder / self.config['version_control']['dvc']['repository_path']
        self.metadata_path = self.base_folder / self.config['metadata']['metadata_dir']
        
        self.validation_errors = []
        self.validation_warnings = []
    
    def validate_git_repository(self) -> bool:
        """Validate Git repository integrity"""
        self.logger.info("Validating Git repository...")
        
        if not (self.git_path / '.git').exists():
            self.validation_errors.append("Git repository not initialized")
            return False
        
        try:
            # Check Git status
            result = subprocess.run(['git', 'status'], cwd=self.git_path, 
                                  capture_output=True, text=True, check=True)
            
            # Check for uncommitted changes
            if "nothing to commit" not in result.stdout:
                self.validation_warnings.append("Git repository has uncommitted changes")
            
            # Validate remote configuration
            try:
                subprocess.run(['git', 'remote', '-v'], cwd=self.git_path,
                              capture_output=True, text=True, check=True)
            except subprocess.CalledProcessError:
                self.validation_warnings.append("No Git remote configured")
            
            self.logger.info("Git repository validation passed")
            return True
            
        except subprocess.CalledProcessError as e:
            self.validation_errors.append(f"Git repository validation failed: {e}")
            return False
    
    def validate_dvc_repository(self) -> bool:
        """Validate DVC repository integrity"""
        self.logger.info("Validating DVC repository...")
        
        if not (self.dvc_path / '.dvc').exists():
            self.validation_errors.append("DVC repository not initialized")
            return False
        
        try:
            # Check DVC status
            result = subprocess.run(['dvc', 'status'], cwd=self.dvc_path,
                                  capture_output=True, text=True, check=True)
            
            # Check for DVC pipeline
            dvc_yaml = self.dvc_path / 'dvc.yaml'
            if not dvc_yaml.exists():
                self.validation_warnings.append("DVC pipeline not defined (dvc.yaml missing)")
            
            # Check DVC cache
            cache_dir = self.dvc_path / '.dvc' / 'cache'
            if cache_dir.exists():
                cache_size = sum(f.stat().st_size for f in cache_dir.rglob('*') if f.is_file())
                self.logger.info(f"DVC cache size: {cache_size / (1024*1024):.2f} MB")
            
            self.logger.info("DVC repository validation passed")
            return True
            
        except subprocess.CalledProcessError as e:
            self.validation_errors.append(f"DVC repository validation failed: {e}")
            return False
    
    def validate_version_metadata(self) -> bool:
        """Validate version metadata integrity"""
        self.logger.info("Validating version metadata...")
        
        version_dir = self.metadata_path / 'versions'
        if not version_dir.exists():
            self.validation_warnings.append("No version metadata found")
            return True
        
        valid_count = 0
        total_count = 0
        
        for version_file in version_dir.glob('v*.json'):
            total_count += 1
            try:
                with open(version_file, 'r') as f:
                    metadata = json.load(f)
                
                # Validate required fields
                required_fields = ['version', 'timestamp', 'files', 'author']
                for field in required_fields:
                    if field not in metadata:
                        self.validation_errors.append(f"Missing required field '{field}' in {version_file.name}")
                        continue
                
                # Validate file hashes
                for file_info in metadata.get('files', []):
                    if 'hash' in file_info and 'path' in file_info:
                        file_path = self.base_folder.parent / file_info['path']
                        if file_path.exists():
                            actual_hash = self.calculate_file_hash(file_path)
                            if actual_hash != file_info['hash']:
                                self.validation_errors.append(f"File hash mismatch: {file_info['path']}")
                        else:
                            self.validation_warnings.append(f"Referenced file not found: {file_info['path']}")
                
                valid_count += 1
                
            except Exception as e:
                self.validation_errors.append(f"Invalid metadata file {version_file.name}: {e}")
        
        self.logger.info(f"Validated {valid_count}/{total_count} version metadata files")
        return len(self.validation_errors) == 0
    
    def calculate_file_hash(self, file_path: pathlib.Path) -> str:
        """Calculate SHA-256 hash of a file"""
        sha256_hash = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
            return sha256_hash.hexdigest()
        except Exception:
            return ""
    
    def validate_data_integrity(self) -> bool:
        """Validate data file integrity across all steps"""
        self.logger.info("Validating data integrity...")
        
        integrity_issues = 0
        
        for step_name, step_config in self.config['data_sources'].items():
            step_path = self.base_folder / step_config['path']
            if not step_path.exists():
                self.validation_warnings.append(f"Step path does not exist: {step_path}")
                continue
            
            patterns = step_config['patterns']
            for pattern in patterns:
                files = list(step_path.rglob(pattern))
                
                for file_path in files:
                    try:
                        # Check file accessibility
                        with open(file_path, 'rb') as f:
                            f.read(1024)  # Read first 1KB to check accessibility
                        
                        # Check file size
                        file_size = file_path.stat().st_size
                        if file_size == 0:
                            self.validation_warnings.append(f"Empty file: {file_path}")
                        
                    except Exception as e:
                        self.validation_errors.append(f"File integrity issue {file_path}: {e}")
                        integrity_issues += 1
        
        if integrity_issues == 0:
            self.logger.info("Data integrity validation passed")
            return True
        else:
            self.logger.error(f"Found {integrity_issues} data integrity issues")
            return False
    
    def validate_configuration(self) -> bool:
        """Validate configuration consistency"""
        self.logger.info("Validating configuration...")
        
        # Check required configuration sections
        required_sections = ['version_control', 'data_sources', 'versioning', 'metadata']
        for section in required_sections:
            if section not in self.config:
                self.validation_errors.append(f"Missing configuration section: {section}")
        
        # Validate data source paths
        for step_name, step_config in self.config['data_sources'].items():
            if 'path' not in step_config:
                self.validation_errors.append(f"Missing path in {step_name} configuration")
            if 'patterns' not in step_config:
                self.validation_errors.append(f"Missing patterns in {step_name} configuration")
        
        # Validate version control configuration
        vc_config = self.config.get('version_control', {})
        if 'git' not in vc_config or 'dvc' not in vc_config:
            self.validation_errors.append("Incomplete version control configuration")
        
        if len(self.validation_errors) == 0:
            self.logger.info("Configuration validation passed")
            return True
        else:
            return False
    
    def validate_dependencies(self) -> bool:
        """Validate external dependencies"""
        self.logger.info("Validating external dependencies...")
        
        # Check Git availability
        try:
            subprocess.run(['git', '--version'], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            self.validation_errors.append("Git not available or not installed")
        
        # Check DVC availability
        try:
            subprocess.run(['dvc', '--version'], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            self.validation_errors.append("DVC not available or not installed")
        
        # Check Python dependencies
        required_packages = ['pandas', 'pyyaml', 'gitpython']
        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                self.validation_errors.append(f"Required Python package not available: {package}")
        
        if len(self.validation_errors) == 0:
            self.logger.info("Dependencies validation passed")
            return True
        else:
            return False
    
    def run_validation(self, check_all: bool = True, verbose: bool = False) -> Dict[str, Any]:
        """Run comprehensive validation"""
        self.logger.info("Starting version control validation...")
        
        validation_results = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'UNKNOWN',
            'checks': {},
            'errors': [],
            'warnings': []
        }
        
        checks = [
            ('configuration', self.validate_configuration),
            ('dependencies', self.validate_dependencies),
            ('git_repository', self.validate_git_repository),
            ('dvc_repository', self.validate_dvc_repository),
            ('version_metadata', self.validate_version_metadata),
            ('data_integrity', self.validate_data_integrity)
        ]
        
        passed_checks = 0
        total_checks = len(checks)
        
        for check_name, check_function in checks:
            if check_all or check_name in ['configuration', 'dependencies']:
                try:
                    result = check_function()
                    validation_results['checks'][check_name] = 'PASS' if result else 'FAIL'
                    if result:
                        passed_checks += 1
                    if verbose:
                        self.logger.info(f"Check '{check_name}': {'PASS' if result else 'FAIL'}")
                except Exception as e:
                    validation_results['checks'][check_name] = 'ERROR'
                    self.validation_errors.append(f"Validation check {check_name} failed: {e}")
                    if verbose:
                        self.logger.error(f"Check '{check_name}': ERROR - {e}")
        
        # Determine overall status
        if len(self.validation_errors) == 0:
            validation_results['overall_status'] = 'PASS'
        elif passed_checks >= total_checks * 0.8:  # 80% pass rate
            validation_results['overall_status'] = 'WARNING'
        else:
            validation_results['overall_status'] = 'FAIL'
        
        # Add errors and warnings
        validation_results['errors'] = self.validation_errors
        validation_results['warnings'] = self.validation_warnings
        
        # Save validation report
        report_path = self.base_folder / self.config['output']['reports_dir'] / f"validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(report_path, 'w') as f:
            json.dump(validation_results, f, indent=2)
        
        # Log summary
        self.logger.info(f"Validation completed: {validation_results['overall_status']}")
        self.logger.info(f"Passed checks: {passed_checks}/{total_checks}")
        if self.validation_errors:
            self.logger.error(f"Errors: {len(self.validation_errors)}")
        if self.validation_warnings:
            self.logger.warning(f"Warnings: {len(self.validation_warnings)}")
        
        rel_report_path = os.path.relpath(str(report_path), os.getcwd())
        self.logger.info(f"Validation report saved: {rel_report_path}")
        
        return validation_results

def load_config(script_folder):
    config_path = script_folder.parent / 'config_step8.yaml'
    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found at {config_path}")
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def setup_logging(base_path, config):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = base_path / config['output']['logs_dir']
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / f"version_validation_{timestamp}.log"
    
    formatter = logging.Formatter(
        config['logging']['format'],
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger = logging.getLogger(__name__)
    logger.setLevel(getattr(logging, config['logging']['level']))
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger

def main():
    parser = argparse.ArgumentParser(description='Version Control Validator')
    parser.add_argument('--check-all', action='store_true',
                       help='Run all validation checks')
    parser.add_argument('--verbose', action='store_true',
                       help='Verbose output')
    parser.add_argument('--config-only', action='store_true',
                       help='Check configuration only')
    
    args = parser.parse_args()
    
    script_folder = pathlib.Path(__file__).parent.resolve()
    project_root = script_folder.parent.parent.resolve()
    
    try:
        config = load_config(script_folder)
        logger = setup_logging(script_folder.parent, config)
        
        validator = VersionValidator(logger, script_folder, config)
        
        # Determine what to check
        check_all = args.check_all if hasattr(args, 'check_all') else True
        if args.config_only:
            check_all = False
        
        results = validator.run_validation(check_all, args.verbose)
        
        # Print summary
        print(f"Validation Status: {results['overall_status']}")
        print(f"Checks Results:")
        for check_name, status in results['checks'].items():
            print(f"  {check_name}: {status}")
        
        if results['errors']:
            print("\nErrors:")
            for error in results['errors']:
                print(f"  - {error}")
        
        if results['warnings']:
            print("\nWarnings:")
            for warning in results['warnings']:
                print(f"  - {warning}")
        
        # Exit with appropriate code
        if results['overall_status'] == 'FAIL':
            sys.exit(1)
        elif results['overall_status'] == 'WARNING':
            sys.exit(2)
        else:
            sys.exit(0)
            
    except Exception as e:
        print(f"Validation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()