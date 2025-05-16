#!/usr/bin/env python
"""
Create Config Layer

This script creates a Lambda layer containing the project configuration.
"""

import os
import sys
import shutil
import tempfile
import zipfile
from pathlib import Path

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[2]))


def create_config_layer(output_dir: str) -> str:
    """
    Create a Lambda layer containing the project configuration.
    
    Args:
        output_dir: Directory to save the layer zip file
    
    Returns:
        str: Path to the layer zip file
    """
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create the Python directory structure
        python_dir = os.path.join(temp_dir, 'python')
        os.makedirs(python_dir, exist_ok=True)
        
        # Copy the config.py file
        config_path = Path(__file__).resolve().parents[2] / 'config.py'
        shutil.copy(config_path, python_dir)
        
        # Create the layer zip file
        output_path = os.path.join(output_dir, 'config_layer.zip')
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(temp_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, temp_dir)
                    zipf.write(file_path, arcname)
        
        print(f"Created config layer: {output_path}")
        return output_path


def main():
    """Main function."""
    # Get the output directory
    script_dir = Path(__file__).resolve().parent
    output_dir = script_dir / 'layers'
    os.makedirs(output_dir, exist_ok=True)
    
    # Create the config layer
    create_config_layer(str(output_dir))


if __name__ == '__main__':
    main()
