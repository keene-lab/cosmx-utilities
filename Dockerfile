# Multi-stage Dockerfile for napari-cosmx-fork
# Builds two targets:
# 1. headless: CLI tools + AWS CLI for Fargate (stitch-images, read-targets)
# 2. gui: Full napari with Qt for desktop environments (Klone)

# =============================================================================
# Stage 1: Base - Common dependencies
# =============================================================================
FROM astral/uv:python3.10-bookworm AS base

WORKDIR /app

# Install minimal build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpcre3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace files
COPY pyproject.toml uv.lock ./
COPY napari-cosmx-fork/ ./napari-cosmx-fork/

# =============================================================================
# Stage 2: Headless - CLI tools only (NO Qt, NO napari GUI)
# =============================================================================
FROM base AS headless

# Install AWS CLI for S3 operations in Fargate
RUN apt-get update && apt-get install -y --no-install-recommends \
    awscli \
    && rm -rf /var/lib/apt/lists/*

# Install all dependencies including workspace to uv's venv
# No [gui] extras means no napari, no Qt
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

# Copy wrapper scripts for Fargate orchestration
COPY scripts/ /app/scripts/
RUN chmod +x /app/scripts/*.sh

# Set default entrypoint
ENTRYPOINT ["uv", "run"]
CMD ["stitch-images", "--help"]

# =============================================================================
# Stage 3: GUI - Full napari with Qt support (for Klone)
# =============================================================================
FROM base AS gui

# Install X11 and OpenGL libraries for GUI support
# PyQt6 bundles Qt6 in the wheel, so no Qt dev tools needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    # OpenGL libraries
    libgl1-mesa-glx \
    libgl1-mesa-dev \
    libglib2.0-0 \
    # X11 and xcb libraries (required for Qt on Linux)
    libxkbcommon-x11-0 \
    libxcb-icccm4 \
    libxcb-image0 \
    libxcb-keysyms1 \
    libxcb-randr0 \
    libxcb-render-util0 \
    libxcb-xinerama0 \
    libxcb-xfixes0 \
    libxcb-shape0 \
    libxcb-cursor0 \
    libx11-xcb1 \
    libxrender1 \
    libxi6 \
    libsm6 \
    libice6 \
    # Font libraries
    libfontconfig1 \
    libfreetype6 \
    # DBus (needed by Qt6)
    libdbus-1-3 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies including napari with full GUI support
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project

# Install workspace root WITH [gui] extras (includes napari and Qt)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -e ".[gui]"

# Configure for GUI operation (can be overridden at runtime)
ENV QT_QPA_PLATFORM=xcb
ENV MPLBACKEND=QtAgg

# Set default entrypoint
# Use system napari directly to avoid uv rebuilding the package
ENTRYPOINT ["napari"]
CMD []
