#!/bin/bash
# Quick test script for Docker images

set -e

echo "========================================="
echo "Testing Headless Image"
echo "========================================="

echo "Building headless image..."
docker build --target headless -t napari-cosmx:headless . -q

echo "Checking napari NOT installed..."
docker run --rm napari-cosmx:headless python -c "import napari" 2>&1 | grep -q "ModuleNotFoundError" || (echo "FAIL: napari found" && exit 1)

echo "Checking Qt5 NOT installed..."
docker run --rm napari-cosmx:headless python -c "from PyQt5.QtWidgets import QApplication" 2>&1 | grep -q "ModuleNotFoundError" || (echo "FAIL: Qt5 found" && exit 1)

echo "Checking CLI tools..."
docker run --rm napari-cosmx:headless stitch-images --help > /dev/null
docker run --rm napari-cosmx:headless read-targets --help > /dev/null
docker run --rm napari-cosmx:headless create-anndata --help > /dev/null

echo "Checking napari_cosmx import..."
docker run --rm napari-cosmx:headless python -c "import napari_cosmx; assert napari_cosmx.napari_get_reader is None" || (echo "FAIL: napari_get_reader not None" && exit 1)

echo ""
echo "========================================="
echo "Testing GUI Image"
echo "========================================="

echo "Building GUI image..."
docker build --target gui -t napari-cosmx:gui . -q

echo "Checking napari IS installed..."
docker run --rm napari-cosmx:gui python -c "import napari; print(f'napari {napari.__version__}')" || (echo "FAIL: napari not found" && exit 1)

echo "Checking Qt5 IS installed..."
docker run --rm napari-cosmx:gui python -c "from PyQt5.QtWidgets import QApplication" || (echo "FAIL: Qt5 not found" && exit 1)

echo "Checking CLI tools still work..."
docker run --rm napari-cosmx:gui stitch-images --help > /dev/null
docker run --rm napari-cosmx:gui read-targets --help > /dev/null

echo "Checking napari command..."
docker run --rm napari-cosmx:gui napari --help > /dev/null

echo "Checking plugin registration..."
docker run --rm napari-cosmx:gui python -c "
from npe2 import PluginManager
pm = PluginManager.instance()
pm.discover()
plugins = [p.name for p in pm.iter_manifests()]
assert 'napari-cosmx-fork' in plugins, f'Plugin not found in {plugins}'
print(f'Found plugin: napari-cosmx-fork')
" || (echo "FAIL: plugin not registered" && exit 1)

echo ""
echo "========================================="
echo "All tests passed!"
echo "========================================="
echo ""
echo "Image sizes:"
docker images | grep "napari-cosmx"
echo ""
echo "Next steps:"
echo "  1. Review images: docker images | grep napari-cosmx"
echo "  2. Test with real data (see TESTING.md)"
echo "  3. Publish images (see PUBLISHING.md)"
