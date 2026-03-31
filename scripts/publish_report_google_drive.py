from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request


def _fail(msg: str) -> int:
    os.makedirs("reports", exist_ok=True)
    with open(os.path.join("reports", "google_drive_link.txt"), "w", encoding="utf-8", newline="\n") as f:
        f.write(f"UNPUBLISHED: {msg}\n")
    return 2


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pdf", required=True, help="Path to PDF to upload")
    args = parser.parse_args()

    access_token = os.environ.get("GDRIVE_ACCESS_TOKEN")
    folder_id = os.environ.get("GDRIVE_FOLDER_ID")
    if not access_token or not folder_id:
        return _fail("Set GDRIVE_ACCESS_TOKEN and GDRIVE_FOLDER_ID env vars.")
    if not os.path.exists(args.pdf):
        return _fail(f"PDF not found: {args.pdf}")

    # 1) Create file metadata
    meta = {"name": os.path.basename(args.pdf), "parents": [folder_id]}
    boundary = "week7_enforcer_boundary"

    def part(headers: dict[str, str], body: bytes) -> bytes:
        h = "".join([f"{k}: {v}\r\n" for k, v in headers.items()])
        return (f"--{boundary}\r\n{h}\r\n").encode("utf-8") + body + b"\r\n"

    with open(args.pdf, "rb") as f:
        pdf_bytes = f.read()

    multipart = b"".join(
        [
            part({"Content-Type": "application/json; charset=UTF-8"}, json.dumps(meta).encode("utf-8")),
            part({"Content-Type": "application/pdf"}, pdf_bytes),
            f"--{boundary}--\r\n".encode("utf-8"),
        ]
    )

    upload_url = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart"
    req = urllib.request.Request(
        upload_url,
        data=multipart,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": f"multipart/related; boundary={boundary}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return _fail(f"HTTP {e.code}: {e.read().decode('utf-8', errors='ignore')}")
    except Exception as e:
        return _fail(str(e))

    file_id = data.get("id")
    if not file_id:
        return _fail("Upload succeeded but no file id returned.")

    # 2) Create shareable link (anyone with link can read)
    perm_url = f"https://www.googleapis.com/drive/v3/files/{file_id}/permissions"
    perm_req = urllib.request.Request(
        perm_url,
        data=json.dumps({"role": "reader", "type": "anyone"}).encode("utf-8"),
        headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(perm_req, timeout=60).read()
    except Exception as e:
        return _fail(f"Uploaded but failed to set permission: {e}")

    link = f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
    os.makedirs("reports", exist_ok=True)
    with open(os.path.join("reports", "google_drive_link.txt"), "w", encoding="utf-8", newline="\n") as f:
        f.write(link + "\n")
    print(link)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

