# Step 8: Data Versioning Git Workflow and Repository Structure

## Git Repository Structure
```
ml-pipeline-data-versions/
├── .gitignore                    # Git ignore patterns
├── README.md                     # Repository documentation
├── CHANGELOG.md                  # Version history and changes
├── LICENSE                       # Repository license
├── .github/                      # GitHub specific configurations
│   ├── workflows/                # GitHub Actions workflows
│   │   ├── data-versioning.yml   # Automated versioning pipeline
│   │   ├── validation.yml        # Data validation workflow
│   │   └── release.yml           # Release automation
│   ├── ISSUE_TEMPLATE/           # Issue templates
│   └── PULL_REQUEST_TEMPLATE.md  # PR template
├── docs/                         # Documentation
│   ├── versioning-guide.md       # Versioning strategy documentation
│   ├── api-reference.md          # API documentation
│   ├── troubleshooting.md        # Common issues and solutions
│   └── contributing.md           # Contribution guidelines
├── data/                         # Version-controlled data and metadata
│   ├── configs/                  # Pipeline configurations
│   │   ├── step1_config.yaml
│   │   ├── step2_config.yaml
│   │   └── ...
│   ├── metadata/                 # Version and lineage metadata
│   │   ├── versions/             # Version-specific metadata
│   │   ├── lineage/              # Data lineage information
│   │   └── schemas/              # Data schemas
│   ├── reports/                  # Generated reports and documentation
│   └── logs/                     # Pipeline execution logs
├── scripts/                      # Versioning and utility scripts
│   ├── versioning/               # Core versioning scripts
│   ├── validation/               # Data validation scripts
│   └── utilities/                # Helper utilities
└── tags/                         # Version tags and releases
    ├── v1.0.0/
    ├── v1.1.0/
    └── latest/
```

## Git Workflow Strategy

### Branch Strategy
- **main**: Stable production versions
- **develop**: Integration branch for new features  
- **feature/***: Feature development branches
- **hotfix/***: Critical bug fixes
- **release/***: Release preparation branches

### Commit Message Convention
```
[Step {step_number}] {type}({scope}): {description}

{body}

{footer}
```

**Types:**
- `feat`: New feature or functionality
- `fix`: Bug fix or correction
- `data`: Data update or refresh
- `config`: Configuration change
- `docs`: Documentation update
- `refactor`: Code refactoring
- `test`: Test additions or modifications
- `chore`: Maintenance tasks

**Examples:**
```
[Step 6] feat(transformation): Add geographic risk scoring feature

Added new derived feature for geographic risk assessment based on 
customer location data and historical default patterns.

Closes #123
```

```
[Step 5] fix(preparation): Correct outlier detection threshold

Fixed threshold calculation in IQR-based outlier detection that was
causing valid data points to be incorrectly flagged as outliers.

Fixes #456
```

### Tagging Strategy

#### Semantic Versioning
- **v{MAJOR}.{MINOR}.{PATCH}**
  - MAJOR: Breaking changes, schema modifications, new data sources
  - MINOR: New features, parameter changes, algorithm updates
  - PATCH: Data updates, bug fixes, documentation changes

#### Special Tags
- **baseline-{date}**: Initial baseline versions
- **production-v{version}**: Production-ready releases
- **experiment-{name}**: Experimental versions
- **milestone-{name}**: Project milestones

#### Tag Creation Examples
```bash
# Create semantic version tag
git tag -a v1.2.0 -m "Feature store with geographic risk scoring

Added:
- Geographic risk scoring feature
- Enhanced feature metadata management
- Improved API documentation

Data Quality Metrics:
- Completeness: 98.5%
- Consistency: 96.2%
- Validity: 97.8%

Tested with 10,000 customer records"

# Create milestone tag
git tag -a milestone-q1-2025 -m "Q1 2025 milestone: Complete feature engineering pipeline"

# Create production tag
git tag -a production-v1.0.0 -m "Production release v1.0.0 - Validated and tested"
```

## GitHub Actions Workflows

### Data Versioning Workflow (.github/workflows/data-versioning.yml)
```yaml
name: Data Versioning Pipeline
on:
  push:
    branches: [main, develop]
    paths:
      - '**/*.csv'
      - '**/*.json'
      - '**/*.parquet'
      - '**/config_*.yaml'
  pull_request:
    branches: [main]
  schedule:
    - cron: '0 6 * * *'  # Daily at 6 AM UTC
  workflow_dispatch:
    inputs:
      version_type:
        description: 'Version increment type'
        required: true
        default: 'patch'
        type: choice
        options:
          - major
          - minor
          - patch
      description:
        description: 'Version description'
        required: false

jobs:
  validate-data:
    runs-on: ubuntu-latest
    steps:
    - name: Checkout code
      uses: actions/checkout@v4
      with:
        fetch-depth: 0  # Full history for version comparison
    
    - name: Setup Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r 08_Data_Versioning/requirements_step8.txt
    
    - name: Install Git and DVC
      run: |
        sudo apt-get update
        sudo apt-get install -y git
        pip install dvc
    
    - name: Configure Git
      run: |
        git config --global user.name "GitHub Actions"
        git config --global user.email "actions@github.com"
    
    - name: Run data validation
      run: |
        cd 08_Data_Versioning
        python scripts/version_validator.py --check-all --verbose
    
    - name: Run versioning
      run: |
        cd 08_Data_Versioning
        if [ "${{ github.event_name }}" = "workflow_dispatch" ]; then
          python scripts/data_versioning_manager.py --description "${{ github.event.inputs.description }}"
        else
          python scripts/data_versioning_manager.py --init-all
        fi
    
    - name: Upload artifacts
      uses: actions/upload-artifact@v3
      with:
        name: version-reports
        path: |
          08_Data_Versioning/reports/
          08_Data_Versioning/logs/
    
    - name: Create Release
      if: github.ref == 'refs/heads/main' && github.event_name != 'schedule'
      uses: actions/create-release@v1
      env:
        GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
      with:
        tag_name: ${{ steps.version.outputs.version }}
        release_name: Data Version ${{ steps.version.outputs.version }}
        body: |
          ## Changes in this version
          ${{ github.event.inputs.description }}
          
          ## Quality Metrics
          - Files processed: ${{ steps.metrics.outputs.file_count }}
          - Data completeness: ${{ steps.metrics.outputs.completeness }}%
          - Validation status: ${{ steps.validation.outputs.status }}
        draft: false
        prerelease: false
```

### Validation Workflow (.github/workflows/validation.yml)
```yaml
name: Data Validation
on:
  pull_request:
    branches: [main, develop]
  push:
    branches: [feature/*]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - name: Setup Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'
    - name: Install dependencies
      run: pip install -r 08_Data_Versioning/requirements_step8.txt
    - name: Run validation
      run: |
        cd 08_Data_Versioning
        python scripts/version_validator.py --config-only --verbose
    - name: Comment PR
      if: github.event_name == 'pull_request'
      uses: actions/github-script@v6
      with:
        script: |
          const fs = require('fs');
          const report = fs.readFileSync('08_Data_Versioning/reports/validation_report_latest.json', 'utf8');
          const data = JSON.parse(report);
          
          const comment = `## Validation Results
          
          **Overall Status**: ${data.overall_status}
          
          **Checks**:
          ${Object.entries(data.checks).map(([name, status]) => `- ${name}: ${status}`).join('\n')}
          
          ${data.errors.length > 0 ? `**Errors**:\n${data.errors.map(e => `- ${e}`).join('\n')}` : ''}
          ${data.warnings.length > 0 ? `**Warnings**:\n${data.warnings.map(w => `- ${w}`).join('\n')}` : ''}
          `;
          
          github.rest.issues.createComment({
            issue_number: context.issue.number,
            owner: context.repo.owner,
            repo: context.repo.repo,
            body: comment
          });
```

## Git Hooks

### Pre-commit Hook
```bash
#!/bin/bash
# .git/hooks/pre-commit

echo "Running pre-commit data validation..."

cd 08_Data_Versioning

# Run configuration validation
python scripts/version_validator.py --config-only

if [ $? -ne 0 ]; then
    echo "❌ Pre-commit validation failed. Please fix configuration issues."
    exit 1
fi

# Check for large files
find . -type f -size +50M -not -path './.git/*' -not -path './dvc/.dvc/cache/*'
if [ $? -eq 0 ]; then
    echo "⚠️  Large files detected. Consider using DVC for tracking."
fi

echo "✅ Pre-commit validation passed"
exit 0
```

### Pre-push Hook
```bash
#!/bin/bash
# .git/hooks/pre-push

echo "Running pre-push data integrity checks..."

cd 08_Data_Versioning

# Run full validation
python scripts/version_validator.py --check-all

if [ $? -ne 0 ]; then
    echo "❌ Pre-push validation failed. Push cancelled."
    exit 1
fi

echo "✅ Pre-push validation passed"
exit 0
```

## Release Process

### Automated Release Workflow
1. **Feature Development**: Create feature branch
2. **Integration**: Merge to develop branch
3. **Testing**: Automated validation and testing
4. **Release Preparation**: Create release branch
5. **Tagging**: Semantic version tagging
6. **Production**: Deploy to main branch

### Manual Release Steps
```bash
# 1. Prepare release branch
git checkout develop
git pull origin develop
git checkout -b release/v1.2.0

# 2. Update version information
# Edit version files and documentation

# 3. Final testing
cd 08_Data_Versioning
python scripts/version_validator.py --check-all

# 4. Merge to main
git checkout main
git merge release/v1.2.0

# 5. Create version tag
git tag -a v1.2.0 -m "Release v1.2.0: Enhanced feature engineering"

# 6. Push to remote
git push origin main --tags

# 7. Create GitHub release
gh release create v1.2.0 --title "Data Version v1.2.0" --notes "Release notes here"
```

## Repository Management

### .gitignore Template
```bash
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
ENV/

# Data files (use DVC instead)
*.csv
*.parquet
*.db
*.sqlite
!**/schema/*.db
!**/templates/*.csv

# Logs
*.log
logs/

# Temporary files
temp/
tmp/
*.tmp

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# DVC
.dvc/cache/
.dvc/tmp/

# Credentials
*.env
credentials/
secrets/
```

### Repository Templates

#### README.md Template
```markdown
# ML Pipeline Data Versions

Version control repository for machine learning pipeline data and metadata.

## Overview
This repository tracks versions of datasets, configurations, and metadata across all stages of the ML pipeline, ensuring reproducibility and data lineage.

## Quick Start
```bash
git clone https://github.com/your-username/ml-pipeline-data-versions.git
cd ml-pipeline-data-versions
pip install -r requirements.txt
dvc pull  # Download latest data
```

## Version History
- **v1.2.0**: Enhanced feature engineering with geographic risk scoring
- **v1.1.0**: Improved data validation and quality metrics
- **v1.0.0**: Initial production release

## Documentation
- [Versioning Guide](docs/versioning-guide.md)
- [API Reference](docs/api-reference.md)
- [Troubleshooting](docs/troubleshooting.md)

## Contributing
See [CONTRIBUTING.md](docs/contributing.md) for guidelines.
```

#### CHANGELOG.md Template
```markdown
# Changelog
All notable changes to the ML pipeline data will be documented here.

## [1.2.0] - 2025-08-23
### Added
- Geographic risk scoring feature
- Enhanced feature metadata management
- Automated quality monitoring

### Changed
- Improved outlier detection algorithm
- Updated validation thresholds

### Fixed
- Balance calculation bug in derived features
- Memory leak in data preparation stage

## [1.1.0] - 2025-08-20
### Added
- Data drift detection
- Automated validation reports
- Extended API documentation

### Changed
- Optimized feature ingestion performance
- Enhanced error handling

## [1.0.0] - 2025-08-15
### Added
- Initial production release
- Complete pipeline versioning
- Basic monitoring and alerting
```

This comprehensive Git workflow ensures proper version control, collaboration, and release management for your data versioning system.