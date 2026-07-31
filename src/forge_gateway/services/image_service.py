"""Image payload encoding service."""

from __future__ import annotations

import base64
import sys
import threading
from collections import deque
from typing import TYPE_CHECKING, Any, Callable, cast

if TYPE_CHECKING:
    from forge_gateway.services.runtime_service import GatewayRuntime

try:
    from forge_common import get_logger
except Exception:  # pragma: no cover - fallback for minimal test envs
    import logging

    def get_logger(name: str) -> logging.Logger:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(name)

logger = get_logger(__name__)

def _image_payload(input_id: str, value: object, quality: int) -> dict[str, Any] | None:
    from forge_msgs import CompressedImage, Image

    try:
        compressed = CompressedImage.from_arrow(value)  # type: ignore[arg-type]
        fmt = compressed.format.lower()
        content_type = "image/jpeg" if fmt in {"jpg", "jpeg"} else f"image/{fmt}"
        encoded = base64.b64encode(compressed.data).decode("ascii")
        return {
            "type": "image",
            "id": input_id,
            "format": fmt,
            "content_type": content_type,
            "data": encoded,
        }
    except Exception:
        pass

    try:
        import cv2  # type: ignore

        img = Image.from_arrow(value)  # type: ignore[arg-type]
        frame = img.to_numpy()
        if img.encoding == "rgb8" and frame.ndim == 3 and frame.shape[-1] == 3:
            frame = frame[..., ::-1]
        elif img.encoding == "mono8" and frame.ndim == 3 and frame.shape[-1] == 1:
            frame = frame[:, :, 0]
        elif img.encoding in ("16UC1", "32FC1"):
            frame = cv2.normalize(frame, None, 0, 255, cv2.NORM_MINMAX).astype(  # pyright: ignore[reportArgumentType, reportCallIssue]
                "uint8"
            )
        ok, encoded = cv2.imencode(
            ".jpg",
            frame,
            [int(cv2.IMWRITE_JPEG_QUALITY), quality],
        )
        if not ok:
            return None
        data = base64.b64encode(encoded.tobytes()).decode("ascii")
        return {
            "type": "image",
            "id": input_id,
            "format": "jpeg",
            "content_type": "image/jpeg",
            "data": data,
        }
    except Exception as e:
        logger.warning("gateway: encode image %s failed: %s", input_id, e)
        return None


class ImageEncodeWorker:
    """Encode images on a background thread while retaining only latest frames."""

    def __init__(self, runtime: GatewayRuntime) -> None:
        self._runtime = runtime
        self._condition = threading.Condition()
        self._input_order: deque[str] = deque()
        self._latest_inputs: dict[str, tuple[object, float]] = {}
        self._stopped = False
        self._thread = threading.Thread(target=self._run, name="gateway_image_encoder", daemon=True)
        self._thread.start()

    def submit(self, input_id: str, value: object, timestamp: float) -> None:
        with self._condition:
            if self._stopped:
                return
            if input_id not in self._latest_inputs:
                self._input_order.append(input_id)
            self._latest_inputs[input_id] = (value, timestamp)
            self._condition.notify()

    def close(self) -> None:
        with self._condition:
            self._stopped = True
            self._latest_inputs.clear()
            self._input_order.clear()
            self._condition.notify_all()
        if self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def _run(self) -> None:
        while True:
            item = self._next_item()
            if item is None:
                return
            input_id, value, timestamp = item
            try:
                payload = _payload_encoder()(input_id, value, self._runtime.config.jpeg_quality)
                if payload is None:
                    continue
                with self._runtime.lock:
                    payload["seq"] = self._runtime.next_image_seq
                    payload["timestamp"] = timestamp
                    self._runtime.next_image_seq += 1
                    self._runtime.images[input_id] = payload
            except Exception as e:
                logger.warning("gateway: image worker failed for %s: %s", input_id, e)
                with self._runtime.lock:
                    self._runtime.last_error = f"image worker {input_id} failed: {e}"

    def _next_item(self) -> tuple[str, object, float] | None:
        with self._condition:
            while not self._stopped and not self._input_order:
                self._condition.wait()
            if self._stopped:
                return None
            input_id = self._input_order.popleft()
            value, timestamp = self._latest_inputs.pop(input_id)
            return input_id, value, timestamp


ImagePayloadEncoder = Callable[
    [str, object, int],
    dict[str, Any] | None,
]


def _payload_encoder() -> ImagePayloadEncoder:
    main_module = sys.modules.get("main")
    encoder = getattr(main_module, "_image_payload", None) if main_module is not None else None
    if callable(encoder) and encoder is not _image_payload:
        return cast(ImagePayloadEncoder, encoder)
    return _image_payload

