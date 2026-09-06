#!/usr/bin/env bash
set -euo pipefail

export HF_HOME=/mnt/curator-flow/hf-cache
export HF_HUB_OFFLINE=1
export TRANSFORMERS_OFFLINE=1
export PATH=/opt/curator-runtime/venv/bin:/usr/local/cuda-12.8/bin:$PATH
export CUDA_HOME=/usr/local/cuda-12.8
export LD_LIBRARY_PATH=/usr/local/cuda-12.8/lib64:${LD_LIBRARY_PATH:-}

hostname
mountpoint /mnt/curator-flow
command -v ffmpeg ffprobe
/opt/curator-runtime/venv/bin/python --version
/opt/curator-runtime/venv/bin/python - <<'PY'
import ray
import torch
import vllm
from nemo_curator.models.prompt_formatter import PromptFormatter

processor = PromptFormatter("qwen2.5").processor
print("torch", torch.__version__)
print("cuda_available", torch.cuda.is_available())
print("cuda_device", torch.cuda.get_device_name(0))
print("ray", ray.__version__)
print("vllm", vllm.__version__)
print("processor", type(processor).__name__)
PY
