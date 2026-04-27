"""Windows shell integration: context menu verbs and file type icons."""

from __future__ import annotations

import logging
import os
import struct
import sys
import zlib
from pathlib import Path

logger = logging.getLogger(__name__)

EXTENSIONS = {".hdf": "AmiFUSE.DiskImage", ".adf": "AmiFUSE.FloppyImage"}

PROGID_DESCRIPTIONS = {
    "AmiFUSE.DiskImage": "Amiga Disk Image",
    "AmiFUSE.FloppyImage": "Amiga Floppy Image",
}

ICON_DIR = Path(os.environ.get("APPDATA", "")) / "AmiFUSE" / "icons"
_AMIFUSE_DIR = Path(os.environ.get("APPDATA", "")) / "AmiFUSE"
_LAUNCH_VBS = _AMIFUSE_DIR / "launch.vbs"

# VBScript launcher: runs a command with hidden window and no wait,
# avoiding the cmd.exe flash that ``cmd /c start`` causes.
_LAUNCH_VBS_CONTENT = '''\
Set a = WScript.Arguments
cmd = ""
For i = 0 To a.Count - 1
    If i > 0 Then cmd = cmd & " "
    cmd = cmd & """" & a(i) & """"
Next
CreateObject("WScript.Shell").Run cmd, 0, False
'''


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def register(extensions: list[str] | None = None) -> None:
    """Register file associations and context menu verbs for AmiFUSE."""
    if not sys.platform.startswith("win"):
        raise SystemExit("Shell registration is only supported on Windows.")

    import winreg

    exts = _resolve_extensions(extensions)
    launcher = _get_launcher_path()

    for ext in exts:
        progid = EXTENSIONS[ext]
        _register_extension(winreg, ext, progid)
        logger.info("Registered %s -> %s", ext, progid)

    registered_progids: set[str] = set()
    for ext in exts:
        progid = EXTENSIONS[ext]
        if progid not in registered_progids:
            _register_progid(winreg, progid, launcher)
            registered_progids.add(progid)

    _install_icons()
    _notify_shell_change()

    for ext in exts:
        print(f"Registered {ext} with AmiFUSE context menu.")


def unregister(extensions: list[str] | None = None) -> None:
    """Remove AmiFUSE file associations and context menu verbs."""
    if not sys.platform.startswith("win"):
        raise SystemExit("Shell registration is only supported on Windows.")

    import winreg

    exts = _resolve_extensions(extensions)

    removed_progids: set[str] = set()
    for ext in exts:
        progid = EXTENSIONS[ext]
        _unregister_extension(winreg, ext, progid)
        if progid not in removed_progids:
            _delete_key_recursive(
                winreg.HKEY_CURRENT_USER, rf"Software\Classes\{progid}"
            )
            removed_progids.add(progid)
        logger.info("Unregistered %s", ext)

    _remove_icons()
    _notify_shell_change()

    for ext in exts:
        print(f"Unregistered {ext} from AmiFUSE.")


def is_registered() -> bool:
    """Return True if AmiFUSE is registered for .hdf files."""
    if not sys.platform.startswith("win"):
        return False
    import winreg

    try:
        key = winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"Software\Classes\.hdf\OpenWithProgids",
        )
        try:
            winreg.QueryValueEx(key, "AmiFUSE.DiskImage")
            return True
        except FileNotFoundError:
            return False
        finally:
            winreg.CloseKey(key)
    except OSError:
        return False


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _resolve_extensions(extensions: list[str] | None) -> list[str]:
    if extensions is None:
        return list(EXTENSIONS)
    for ext in extensions:
        if ext not in EXTENSIONS:
            raise ValueError(f"Unknown extension: {ext}")
    return extensions


def _get_launcher_path() -> str:
    return str(Path(sys.executable).parent / "amifuse-launcher.exe")


def _register_extension(winreg, ext: str, progid: str) -> None:
    """Register a single extension under HKCU\\Software\\Classes."""
    ext_path = rf"Software\Classes\{ext}"

    key = winreg.CreateKey(winreg.HKEY_CURRENT_USER, ext_path)
    try:
        existing = ""
        try:
            existing, _ = winreg.QueryValueEx(key, "")
        except FileNotFoundError:
            pass

        if existing and existing != progid:
            logger.warning(
                "%s already claimed by %s; adding to OpenWithProgids only",
                ext,
                existing,
            )
            print(
                f"Warning: {ext} is already associated with {existing}. "
                f"Adding AmiFUSE to Open With list without overriding."
            )
        else:
            winreg.SetValueEx(key, "", 0, winreg.REG_SZ, progid)
    finally:
        winreg.CloseKey(key)

    # Always add to OpenWithProgids
    owp_key = winreg.CreateKey(winreg.HKEY_CURRENT_USER, rf"{ext_path}\OpenWithProgids")
    try:
        winreg.SetValueEx(owp_key, progid, 0, winreg.REG_SZ, "")
    finally:
        winreg.CloseKey(owp_key)


def _register_progid(winreg, progid: str, launcher: str) -> None:
    """Create ProgID with flat verb entries and icon."""
    base = rf"Software\Classes\{progid}"
    description = PROGID_DESCRIPTIONS[progid]

    # (Default) = description
    key = winreg.CreateKey(winreg.HKEY_CURRENT_USER, base)
    try:
        winreg.SetValueEx(key, "", 0, winreg.REG_SZ, description)
    finally:
        winreg.CloseKey(key)

    # Flat verb: mount — wscript runs VBS launcher invisibly (no cmd.exe flash)
    vbs = str(_LAUNCH_VBS)
    _set_verb(
        winreg,
        base,
        "mount",
        "Mount with AmiFUSE",
        f'wscript.exe //nologo //b "{vbs}" "{launcher}" mount "%1"',
    )

    # Flat verb: mountrw
    _set_verb(
        winreg,
        base,
        "mountrw",
        "Mount Read-Write with AmiFUSE",
        f'wscript.exe //nologo //b "{vbs}" "{launcher}" mount --write "%1"',
    )

    # DefaultIcon
    icon_name = progid.split(".")[-1].lower() + ".ico"
    icon_path = str(ICON_DIR / icon_name)
    icon_key = winreg.CreateKey(winreg.HKEY_CURRENT_USER, rf"{base}\DefaultIcon")
    try:
        winreg.SetValueEx(icon_key, "", 0, winreg.REG_SZ, icon_path)
    finally:
        winreg.CloseKey(icon_key)


def _set_verb(winreg, base: str, verb: str, label: str, command: str) -> None:
    verb_key = winreg.CreateKey(
        winreg.HKEY_CURRENT_USER, rf"{base}\shell\{verb}"
    )
    try:
        winreg.SetValueEx(verb_key, "", 0, winreg.REG_SZ, label)
    finally:
        winreg.CloseKey(verb_key)

    cmd_key = winreg.CreateKey(
        winreg.HKEY_CURRENT_USER, rf"{base}\shell\{verb}\command"
    )
    try:
        winreg.SetValueEx(cmd_key, "", 0, winreg.REG_SZ, command)
    finally:
        winreg.CloseKey(cmd_key)


def _unregister_extension(winreg, ext: str, progid: str) -> None:
    """Remove AmiFUSE entries from a single extension key."""
    ext_path = rf"Software\Classes\{ext}"

    # Remove from OpenWithProgids
    try:
        owp_key = winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            rf"{ext_path}\OpenWithProgids",
            0,
            winreg.KEY_SET_VALUE | winreg.KEY_READ,
        )
        try:
            try:
                winreg.DeleteValue(owp_key, progid)
            except FileNotFoundError:
                pass

            # If no other ProgIDs remain, delete the key
            try:
                winreg.EnumValue(owp_key, 0)
            except OSError:
                winreg.CloseKey(owp_key)
                owp_key = None
                try:
                    winreg.DeleteKey(
                        winreg.HKEY_CURRENT_USER,
                        rf"{ext_path}\OpenWithProgids",
                    )
                except OSError:
                    pass
        finally:
            if owp_key is not None:
                winreg.CloseKey(owp_key)
    except OSError:
        pass

    # Only clear (Default) if it's ours
    try:
        key = winreg.OpenKey(
            winreg.HKEY_CURRENT_USER, ext_path, 0, winreg.KEY_READ | winreg.KEY_SET_VALUE
        )
        try:
            current, _ = winreg.QueryValueEx(key, "")
            if current == progid:
                winreg.SetValueEx(key, "", 0, winreg.REG_SZ, "")
        except FileNotFoundError:
            pass
        finally:
            winreg.CloseKey(key)
    except OSError:
        pass


def _delete_key_recursive(hkey, sub_key: str) -> None:
    """Recursively delete a registry key and all its children."""
    import winreg

    try:
        key = winreg.OpenKey(hkey, sub_key, 0, winreg.KEY_READ)
    except FileNotFoundError:
        return
    except OSError:
        return

    children = []
    try:
        i = 0
        while True:
            try:
                children.append(winreg.EnumKey(key, i))
                i += 1
            except OSError:
                break
    finally:
        winreg.CloseKey(key)

    for child in children:
        _delete_key_recursive(hkey, rf"{sub_key}\{child}")

    try:
        winreg.DeleteKey(hkey, sub_key)
    except OSError as exc:
        logger.warning("Failed to delete registry key %s: %s", sub_key, exc)


# ---------------------------------------------------------------------------
# Icons — pixel-art file-type icons for HDF and ADF
# ---------------------------------------------------------------------------

# Color palettes
_HDF_COLORS = {
    "body": (0x46, 0x82, 0xB4, 255),       # steel blue
    "highlight": (0x6C, 0xA6, 0xCD, 255),   # lighter blue
    "dark": (0x36, 0x64, 0x8B, 255),         # darker blue
    "outline": (0, 0, 0, 128),               # semi-transparent black
    "label": (0x7E, 0xBA, 0xDB, 255),        # light label area
    "spindle": (0x2A, 0x4E, 0x6E, 255),      # dark circle
    "clear": (0, 0, 0, 0),                   # transparent
}

_ADF_COLORS = {
    "body": (0x3C, 0xB3, 0x71, 255),        # medium sea green
    "highlight": (0x66, 0xCD, 0xAA, 255),    # lighter green
    "dark": (0x2E, 0x8B, 0x57, 255),         # darker green
    "outline": (0, 0, 0, 128),
    "slider": (0xC0, 0xC0, 0xC0, 255),       # silver metal slider
    "slider_dark": (0x90, 0x90, 0x90, 255),   # slider shadow
    "label": (0xF5, 0xF5, 0xDC, 255),        # beige label
    "label_line": (0xCC, 0xCC, 0xAA, 255),   # label lines
    "clear": (0, 0, 0, 0),
}

T = tuple[int, int, int, int]  # BGRA or RGBA pixel type alias (used internally)


def _new_canvas(size: int) -> list[list[T]]:
    """Create a size x size transparent canvas (RGBA)."""
    return [[(0, 0, 0, 0)] * size for _ in range(size)]


def _fill_rect(canvas: list[list[T]], x0: int, y0: int, x1: int, y1: int, color: T) -> None:
    """Fill a rectangle [x0,x1) x [y0,y1) with color, alpha-blending over existing."""
    h = len(canvas)
    w = len(canvas[0]) if h else 0
    for y in range(max(0, y0), min(h, y1)):
        for x in range(max(0, x0), min(w, x1)):
            canvas[y][x] = _blend(canvas[y][x], color)


def _set_pixel(canvas: list[list[T]], x: int, y: int, color: T) -> None:
    h = len(canvas)
    w = len(canvas[0]) if h else 0
    if 0 <= x < w and 0 <= y < h:
        canvas[y][x] = _blend(canvas[y][x], color)


def _blend(bg: T, fg: T) -> T:
    """Alpha-blend fg over bg (both RGBA tuples)."""
    fa = fg[3] / 255.0
    ba = bg[3] / 255.0
    oa = fa + ba * (1 - fa)
    if oa == 0:
        return (0, 0, 0, 0)
    r = int((fg[0] * fa + bg[0] * ba * (1 - fa)) / oa)
    g = int((fg[1] * fa + bg[1] * ba * (1 - fa)) / oa)
    b = int((fg[2] * fa + bg[2] * ba * (1 - fa)) / oa)
    return (r, g, b, int(oa * 255))


def _draw_outline_rect(canvas: list[list[T]], x0: int, y0: int, x1: int, y1: int, color: T) -> None:
    """Draw a 1px outline rectangle."""
    for x in range(x0, x1):
        _set_pixel(canvas, x, y0, color)
        _set_pixel(canvas, x, y1 - 1, color)
    for y in range(y0, y1):
        _set_pixel(canvas, x0, y, color)
        _set_pixel(canvas, x1 - 1, y, color)


def _draw_hdf_16(c: dict[str, T]) -> list[list[T]]:
    """16x16 hard drive: simplified rectangle with dot."""
    canvas = _new_canvas(16)
    # Body: 2,2 to 13,13
    _fill_rect(canvas, 2, 3, 14, 13, c["body"])
    # Top highlight
    _fill_rect(canvas, 2, 3, 14, 5, c["highlight"])
    # Bottom edge
    _fill_rect(canvas, 2, 12, 14, 13, c["dark"])
    # Outline
    _draw_outline_rect(canvas, 2, 3, 14, 13, c["outline"])
    # Label line
    _fill_rect(canvas, 4, 5, 12, 6, c["label"])
    # Spindle dot
    _set_pixel(canvas, 11, 10, c["spindle"])
    _set_pixel(canvas, 10, 10, c["spindle"])
    _set_pixel(canvas, 11, 9, c["spindle"])
    _set_pixel(canvas, 10, 9, c["spindle"])
    return canvas


def _draw_hdf_32(c: dict[str, T]) -> list[list[T]]:
    """32x32 hard drive."""
    canvas = _new_canvas(32)
    # Body
    _fill_rect(canvas, 3, 5, 29, 27, c["body"])
    # Top highlight area (label)
    _fill_rect(canvas, 3, 5, 29, 10, c["highlight"])
    # Label line
    _fill_rect(canvas, 5, 7, 27, 9, c["label"])
    # Bottom darker edge
    _fill_rect(canvas, 3, 25, 29, 27, c["dark"])
    # Outline
    _draw_outline_rect(canvas, 3, 5, 29, 27, c["outline"])
    # Spindle circle (bottom right area)
    for dy in range(-2, 3):
        for dx in range(-2, 3):
            if dx * dx + dy * dy <= 5:
                _set_pixel(canvas, 24 + dx, 21 + dy, c["spindle"])
    # Spindle center highlight
    _set_pixel(canvas, 24, 21, c["dark"])
    return canvas


def _draw_hdf_48(c: dict[str, T]) -> list[list[T]]:
    """48x48 hard drive."""
    canvas = _new_canvas(48)
    # Body
    _fill_rect(canvas, 5, 8, 43, 40, c["body"])
    # Top highlight area
    _fill_rect(canvas, 5, 8, 43, 16, c["highlight"])
    # Label area
    _fill_rect(canvas, 8, 10, 40, 14, c["label"])
    # Bottom darker edge
    _fill_rect(canvas, 5, 37, 43, 40, c["dark"])
    # Side darker edges
    _fill_rect(canvas, 5, 8, 7, 40, c["dark"])
    _fill_rect(canvas, 41, 8, 43, 40, c["dark"])
    # Outline
    _draw_outline_rect(canvas, 5, 8, 43, 40, c["outline"])
    # Horizontal divider line below label area
    _fill_rect(canvas, 6, 16, 42, 17, c["outline"])
    # Spindle circle
    cx, cy = 36, 31
    for dy in range(-4, 5):
        for dx in range(-4, 5):
            dist = dx * dx + dy * dy
            if dist <= 16:
                col = c["spindle"] if dist <= 4 else c["dark"]
                _set_pixel(canvas, cx + dx, cy + dy, col)
    # Spindle center
    _set_pixel(canvas, cx, cy, c["highlight"])
    # Screw holes in corners
    for sx, sy in [(8, 37), (39, 37), (8, 10)]:
        _set_pixel(canvas, sx, sy, c["spindle"])
    return canvas


def _draw_adf_16(c: dict[str, T]) -> list[list[T]]:
    """16x16 floppy disk: square with notch and slider."""
    canvas = _new_canvas(16)
    # Body
    _fill_rect(canvas, 2, 2, 14, 14, c["body"])
    # Top-right notch (write-protect area)
    _fill_rect(canvas, 12, 2, 14, 5, c["clear"])
    # Outline
    for x in range(2, 14):
        _set_pixel(canvas, x, 2, c["outline"])
        _set_pixel(canvas, x, 13, c["outline"])
    for y in range(2, 14):
        _set_pixel(canvas, 2, y, c["outline"])
        _set_pixel(canvas, 13, y, c["outline"])
    # Notch outline
    _set_pixel(canvas, 11, 2, c["outline"])
    _set_pixel(canvas, 11, 3, c["outline"])
    _set_pixel(canvas, 11, 4, c["outline"])
    _set_pixel(canvas, 12, 4, c["outline"])
    _set_pixel(canvas, 13, 4, c["outline"])
    # Clear the notch interior outline edge
    _fill_rect(canvas, 12, 2, 14, 4, c["clear"])
    # Metal slider at top
    _fill_rect(canvas, 5, 2, 10, 5, c["slider"])
    # Label area
    _fill_rect(canvas, 4, 8, 12, 13, c["label"])
    _draw_outline_rect(canvas, 4, 8, 12, 13, c["dark"])
    return canvas


def _draw_adf_32(c: dict[str, T]) -> list[list[T]]:
    """32x32 floppy disk."""
    canvas = _new_canvas(32)
    # Body
    _fill_rect(canvas, 4, 3, 28, 29, c["body"])
    # Top-right notch
    _fill_rect(canvas, 24, 3, 28, 8, c["clear"])
    # Outline
    _draw_outline_rect(canvas, 4, 3, 28, 29, c["outline"])
    # Notch outline
    for y in range(3, 8):
        _set_pixel(canvas, 23, y, c["outline"])
    for x in range(23, 28):
        _set_pixel(canvas, x, 8, c["outline"])
    # Clear notch area (overwrite outline in notch)
    _fill_rect(canvas, 24, 3, 28, 8, c["clear"])
    # Metal slider
    _fill_rect(canvas, 10, 3, 22, 9, c["slider"])
    _draw_outline_rect(canvas, 10, 3, 22, 9, c["slider_dark"])
    # Slider opening
    _fill_rect(canvas, 14, 4, 18, 8, c["dark"])
    # Label area
    _fill_rect(canvas, 7, 16, 25, 27, c["label"])
    _draw_outline_rect(canvas, 7, 16, 25, 27, c["dark"])
    # Label lines
    for ly in (19, 21, 23):
        _fill_rect(canvas, 9, ly, 23, ly + 1, c["label_line"])
    return canvas


def _draw_adf_48(c: dict[str, T]) -> list[list[T]]:
    """48x48 floppy disk."""
    canvas = _new_canvas(48)
    # Body
    _fill_rect(canvas, 6, 4, 42, 44, c["body"])
    # Top-right notch
    _fill_rect(canvas, 36, 4, 42, 12, c["clear"])
    # Outline
    _draw_outline_rect(canvas, 6, 4, 42, 44, c["outline"])
    # Notch outline
    for y in range(4, 12):
        _set_pixel(canvas, 35, y, c["outline"])
    for x in range(35, 42):
        _set_pixel(canvas, x, 12, c["outline"])
    _fill_rect(canvas, 36, 4, 42, 12, c["clear"])
    # Side bevels
    _fill_rect(canvas, 6, 4, 8, 44, c["dark"])
    _fill_rect(canvas, 40, 4, 42, 44, c["dark"])
    # Metal slider
    _fill_rect(canvas, 14, 4, 34, 14, c["slider"])
    _draw_outline_rect(canvas, 14, 4, 34, 14, c["slider_dark"])
    # Slider opening
    _fill_rect(canvas, 20, 5, 28, 13, c["dark"])
    # Circular hub area at bottom center
    cx, cy = 24, 38
    for dy in range(-3, 4):
        for dx in range(-3, 4):
            if dx * dx + dy * dy <= 9:
                _set_pixel(canvas, cx + dx, cy + dy, c["dark"])
    # Label area
    _fill_rect(canvas, 10, 22, 38, 40, c["label"])
    _draw_outline_rect(canvas, 10, 22, 38, 40, c["dark"])
    # Label lines
    for ly in (26, 29, 32, 35):
        _fill_rect(canvas, 13, ly, 35, ly + 1, c["label_line"])
    return canvas


def _scale_canvas(canvas: list[list[T]], target: int) -> list[list[T]]:
    """Scale a canvas to target size using nearest-neighbor."""
    src_size = len(canvas)
    result = _new_canvas(target)
    for y in range(target):
        sy = y * src_size // target
        for x in range(target):
            sx = x * src_size // target
            result[y][x] = canvas[sy][sx]
    return result


def _canvas_to_bgra(canvas: list[list[T]]) -> bytes:
    """Convert RGBA canvas to BGRA bytes (bottom-up for BMP)."""
    size = len(canvas)
    rows = []
    for y in range(size - 1, -1, -1):  # bottom-up
        for x in range(size):
            r, g, b, a = canvas[y][x]
            rows.append(struct.pack("BBBB", b, g, r, a))
    return b"".join(rows)


def _canvas_to_rgba_topdown(canvas: list[list[T]]) -> bytes:
    """Convert RGBA canvas to RGBA bytes (top-down for PNG)."""
    parts = []
    for row in canvas:
        for r, g, b, a in row:
            parts.append(struct.pack("BBBB", r, g, b, a))
    return b"".join(parts)


def _make_png(canvas: list[list[T]]) -> bytes:
    """Create a PNG file from an RGBA canvas."""
    size = len(canvas)
    # Build raw scanlines: filter byte 0 + RGBA row data
    raw = bytearray()
    for row in canvas:
        raw.append(0)  # filter: None
        for r, g, b, a in row:
            raw.extend((r, g, b, a))
    compressed = zlib.compress(bytes(raw))

    def _chunk(chunk_type: bytes, data: bytes) -> bytes:
        c = chunk_type + data
        return struct.pack(">I", len(data)) + c + struct.pack(">I", zlib.crc32(c) & 0xFFFFFFFF)

    ihdr_data = struct.pack(">IIBBBBB", size, size, 8, 6, 0, 0, 0)
    # 8=bit depth, 6=RGBA, 0=compression, 0=filter, 0=interlace

    return (
        b"\x89PNG\r\n\x1a\n"
        + _chunk(b"IHDR", ihdr_data)
        + _chunk(b"IDAT", compressed)
        + _chunk(b"IEND", b"")
    )


def _make_bmp_entry(canvas: list[list[T]]) -> bytes:
    """Create a BMP DIB for an ICO entry from an RGBA canvas."""
    size = len(canvas)
    bgra = _canvas_to_bgra(canvas)
    bih = struct.pack(
        "<IiiHHIIiiII",
        40, size, size * 2, 1, 32, 0, 0, 0, 0, 0, 0,
    )
    mask_row_bytes = (size + 31) // 32 * 4
    mask = b"\x00" * mask_row_bytes * size
    return bih + bgra + mask


def _make_ico(icon_type: str) -> bytes:
    """Generate a multi-resolution ICO file for the given type ('hdf' or 'adf')."""
    if icon_type == "hdf":
        c = _HDF_COLORS
        draw_16 = _draw_hdf_16
        draw_32 = _draw_hdf_32
        draw_48 = _draw_hdf_48
    else:
        c = _ADF_COLORS
        draw_16 = _draw_adf_16
        draw_32 = _draw_adf_32
        draw_48 = _draw_adf_48

    canvases = {
        16: draw_16(c),
        32: draw_32(c),
        48: draw_48(c),
    }
    # 256x256: scale up from 48x48
    canvases[256] = _scale_canvas(canvases[48], 256)

    entries: list[tuple[int, bytes]] = []
    for size in (16, 32, 48):
        entries.append((size, _make_bmp_entry(canvases[size])))
    # 256x256 as PNG
    entries.append((256, _make_png(canvases[256])))

    header = struct.pack("<HHH", 0, 1, len(entries))
    offset = 6 + 16 * len(entries)
    directory = b""
    image_data = b""

    for size, data in entries:
        w = size if size < 256 else 0
        h = size if size < 256 else 0
        directory += struct.pack(
            "<BBBBHHII",
            w, h, 0, 0, 1, 32, len(data), offset,
        )
        image_data += data
        offset += len(data)

    return header + directory + image_data


_ICON_SPECS = {
    "diskimage.ico": "hdf",
    "floppyimage.ico": "adf",
}


def _install_icons() -> None:
    """Write icon files and launcher script to APPDATA."""
    import tempfile

    ICON_DIR.mkdir(parents=True, exist_ok=True)
    for name, icon_type in _ICON_SPECS.items():
        path = ICON_DIR / name
        data = _make_ico(icon_type)
        try:
            path.write_bytes(data)
        except PermissionError:
            # File may be locked by Explorer; write to temp and atomic-rename
            try:
                fd, tmp = tempfile.mkstemp(dir=ICON_DIR, suffix=".ico")
                os.write(fd, data)
                os.close(fd)
                os.replace(tmp, path)
            except OSError as exc:
                logger.warning("Could not update icon %s (locked): %s", path, exc)
                # Clean up temp file if rename failed
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                continue
        logger.info("Installed icon %s", path)
    # VBS launcher for invisible context-menu invocation
    _LAUNCH_VBS.write_text(_LAUNCH_VBS_CONTENT, encoding="utf-8")
    logger.info("Installed launcher %s", _LAUNCH_VBS)


def _remove_icons() -> None:
    """Delete icon files, launcher script, and clean up empty directories."""
    for name in _ICON_SPECS:
        path = ICON_DIR / name
        try:
            path.unlink()
            logger.info("Removed icon %s", path)
        except FileNotFoundError:
            pass
        except (PermissionError, OSError) as exc:
            logger.warning("Could not remove icon %s (locked): %s", path, exc)

    try:
        _LAUNCH_VBS.unlink()
        logger.info("Removed launcher %s", _LAUNCH_VBS)
    except FileNotFoundError:
        pass
    except (PermissionError, OSError) as exc:
        logger.warning("Could not remove launcher %s: %s", _LAUNCH_VBS, exc)

    # Clean up empty dirs (may fail if files remain due to locks)
    for d in (ICON_DIR, ICON_DIR.parent):
        try:
            d.rmdir()
        except OSError:
            break


# ---------------------------------------------------------------------------
# Shell notification
# ---------------------------------------------------------------------------


def _notify_shell_change() -> None:
    """Tell Explorer that file associations have changed."""
    import ctypes

    SHCNE_ASSOCCHANGED = 0x08000000
    SHCNF_IDLIST = 0x0000
    ctypes.windll.shell32.SHChangeNotify(
        SHCNE_ASSOCCHANGED, SHCNF_IDLIST, None, None
    )
