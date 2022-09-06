# Kubernetes OCR job queue

## Install/update

Check out [pckhoi/k8s-ocr-jobqueue](https://github.com/pckhoi/k8s-ocr-jobqueue)

## Queue PDF for OCR

```bash
scripts/queue_pdf_for_ocr.py PDF_DIRECTORY [PDF_DIRECTORY...]
```

## Check job status

```bash
# see pod status
kubectl get pods -n k8s-ocr-jobqueue -w
# see job log
kubectl logs -n k8s-ocr-jobqueue -f -l job-name=doctr
```

## Download OCR results

```python
from lib.ocr import fetch_ocr_text
...
fetch_ocr_text(files_prefix)
```

## Uninstall job queue

```bash
scripts/uninstall_k8s_ocr_jobqueue.sh
```
