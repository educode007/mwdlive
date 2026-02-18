from pathlib import Path
import struct

SIZE = 256


def clamp(v):
    return max(0, min(255, int(v)))


def make_canvas(size):
    return [[(0, 0, 0, 0) for _ in range(size)] for _ in range(size)]


def set_px(canvas, x, y, color):
    if 0 <= x < SIZE and 0 <= y < SIZE:
        canvas[y][x] = color


def draw_circle(canvas, cx, cy, radius, color, thickness=1):
    r2 = radius * radius
    inner = (radius - thickness) * (radius - thickness)
    for y in range(cy - radius - 1, cy + radius + 2):
        for x in range(cx - radius - 1, cx + radius + 2):
            dx = x - cx
            dy = y - cy
            d2 = dx * dx + dy * dy
            if inner <= d2 <= r2:
                set_px(canvas, x, y, color)


def draw_filled_circle(canvas, cx, cy, radius, color):
    r2 = radius * radius
    for y in range(cy - radius, cy + radius + 1):
        for x in range(cx - radius, cx + radius + 1):
            dx = x - cx
            dy = y - cy
            if (dx * dx + dy * dy) <= r2:
                set_px(canvas, x, y, color)


def draw_line(canvas, x0, y0, x1, y1, color, width=2):
    dx = abs(x1 - x0)
    dy = -abs(y1 - y0)
    sx = 1 if x0 < x1 else -1
    sy = 1 if y0 < y1 else -1
    err = dx + dy
    while True:
        for wy in range(-width, width + 1):
            for wx in range(-width, width + 1):
                if wx * wx + wy * wy <= width * width:
                    set_px(canvas, x0 + wx, y0 + wy, color)
        if x0 == x1 and y0 == y1:
            break
        e2 = 2 * err
        if e2 >= dy:
            err += dy
            x0 += sx
        if e2 <= dx:
            err += dx
            y0 += sy


def draw_bezier(canvas, p0, p1, p2, p3, color, width=2, steps=120):
    prev = p0
    for i in range(1, steps + 1):
        t = i / steps
        mt = 1.0 - t
        x = (
            mt * mt * mt * p0[0]
            + 3 * mt * mt * t * p1[0]
            + 3 * mt * t * t * p2[0]
            + t * t * t * p3[0]
        )
        y = (
            mt * mt * mt * p0[1]
            + 3 * mt * mt * t * p1[1]
            + 3 * mt * t * t * p2[1]
            + t * t * t * p3[1]
        )
        curr = (int(round(x)), int(round(y)))
        draw_line(canvas, prev[0], prev[1], curr[0], curr[1], color, width)
        prev = curr


def draw_arc(canvas, cx, cy, radius, start_deg, end_deg, color, thickness=8):
    steps = max(32, int(abs(end_deg - start_deg) * 2))
    prev = None
    for i in range(steps + 1):
        t = i / steps
        deg = start_deg + (end_deg - start_deg) * t
        rad = deg * 3.141592653589793 / 180.0
        x = int(round(cx + radius * __import__('math').cos(rad)))
        y = int(round(cy + radius * __import__('math').sin(rad)))
        if prev is not None:
            draw_line(canvas, prev[0], prev[1], x, y, color, width=thickness)
        prev = (x, y)


def add_background(canvas):
    for y in range(SIZE):
        for x in range(SIZE):
            if canvas[y][x][3] != 0:
                continue
            v = 24 + int((x / (SIZE - 1)) * 18) + int((y / (SIZE - 1)) * 8)
            canvas[y][x] = (clamp(v), clamp(v + 10), clamp(v + 20), 255)


def build_icon_pixels():
    c = make_canvas(SIZE)
    scale = SIZE / 64.0

    def sc(v):
        return int(round(v * scale))

    # Neutral base like monitor background
    add_background(c)

    cx = sc(32)
    cy = sc(32)
    r = sc(21)

    # Outer arc bands as in monitor compass
    blue = (42, 73, 255, 255)
    maroon = (123, 0, 32, 255)
    draw_arc(c, cx, cy, r + sc(5), -70, 70, blue, thickness=max(1, sc(4)))
    draw_arc(c, cx, cy, r + sc(5), 110, 250, maroon, thickness=max(1, sc(4)))

    # Rings and crosshair
    ring_col = (70, 70, 70, 255)
    for i in range(1, 9):
        draw_circle(c, cx, cy, int(round(r * (i / 8.0))), ring_col, thickness=max(1, sc(0.4)))

    cross = (50, 50, 50, 255)
    draw_line(c, cx - r, cy, cx + r, cy, cross, width=max(1, sc(0.5)))
    draw_line(c, cx, cy - r, cx, cy + r, cross, width=max(1, sc(0.5)))

    # North pointer/needle
    needle = (192, 19, 19, 255)
    draw_line(c, cx, cy, cx, cy - r + sc(2), needle, width=max(1, sc(1.2)))
    draw_line(c, cx, cy - r + sc(2), cx - sc(2), cy - r + sc(6), needle, width=max(1, sc(0.9)))
    draw_line(c, cx, cy - r + sc(2), cx + sc(2), cy - r + sc(6), needle, width=max(1, sc(0.9)))

    # Center cap
    draw_filled_circle(c, cx, cy, sc(2), (245, 245, 245, 255))
    draw_circle(c, cx, cy, sc(2), (160, 160, 160, 255), thickness=max(1, sc(0.5)))

    # Cardinal marks (simple, icon-safe)
    txt = (31, 59, 220, 255)
    # N
    draw_line(c, cx - sc(2), cy - r - sc(4), cx - sc(2), cy - r + sc(1), txt, width=max(1, sc(0.7)))
    draw_line(c, cx + sc(2), cy - r - sc(4), cx + sc(2), cy - r + sc(1), txt, width=max(1, sc(0.7)))
    draw_line(c, cx - sc(2), cy - r - sc(4), cx + sc(2), cy - r + sc(1), txt, width=max(1, sc(0.7)))

    # E
    ex = cx + r + sc(6)
    ey = cy
    draw_line(c, ex - sc(2), ey - sc(3), ex - sc(2), ey + sc(3), txt, width=max(1, sc(0.7)))
    draw_line(c, ex - sc(2), ey - sc(3), ex + sc(2), ey - sc(3), txt, width=max(1, sc(0.7)))
    draw_line(c, ex - sc(2), ey, ex + sc(1), ey, txt, width=max(1, sc(0.7)))
    draw_line(c, ex - sc(2), ey + sc(3), ex + sc(2), ey + sc(3), txt, width=max(1, sc(0.7)))

    # S
    sx = cx
    sy = cy + r + sc(6)
    draw_line(c, sx - sc(2), sy - sc(3), sx + sc(2), sy - sc(3), txt, width=max(1, sc(0.7)))
    draw_line(c, sx - sc(2), sy - sc(3), sx - sc(2), sy, txt, width=max(1, sc(0.7)))
    draw_line(c, sx - sc(2), sy, sx + sc(2), sy, txt, width=max(1, sc(0.7)))
    draw_line(c, sx + sc(2), sy, sx + sc(2), sy + sc(3), txt, width=max(1, sc(0.7)))
    draw_line(c, sx - sc(2), sy + sc(3), sx + sc(2), sy + sc(3), txt, width=max(1, sc(0.7)))

    # W
    wx = cx - r - sc(6)
    wy = cy
    draw_line(c, wx - sc(3), wy - sc(3), wx - sc(2), wy + sc(3), txt, width=max(1, sc(0.7)))
    draw_line(c, wx - sc(2), wy + sc(3), wx, wy, txt, width=max(1, sc(0.7)))
    draw_line(c, wx, wy, wx + sc(2), wy + sc(3), txt, width=max(1, sc(0.7)))
    draw_line(c, wx + sc(2), wy + sc(3), wx + sc(3), wy - sc(3), txt, width=max(1, sc(0.7)))

    return c


def encode_ico(canvas):
    width = SIZE
    height = SIZE

    # XOR bitmap (BGRA), bottom-up rows
    xor_bytes = bytearray()
    for y in range(height - 1, -1, -1):
        for x in range(width):
            r, g, b, a = canvas[y][x]
            xor_bytes.extend([b, g, r, a])

    # AND mask (1 bit/pixel), bottom-up rows, 32-bit padded
    mask_row_bytes = ((width + 31) // 32) * 4
    and_mask = bytearray()
    for y in range(height - 1, -1, -1):
        row = bytearray(mask_row_bytes)
        bit_index = 0
        byte_val = 0
        out_i = 0
        for x in range(width):
            a = canvas[y][x][3]
            bit = 1 if a == 0 else 0
            byte_val = (byte_val << 1) | bit
            bit_index += 1
            if bit_index == 8:
                row[out_i] = byte_val
                out_i += 1
                bit_index = 0
                byte_val = 0
        if bit_index != 0:
            byte_val = byte_val << (8 - bit_index)
            row[out_i] = byte_val
        and_mask.extend(row)

    bi_size = 40
    bi_width = width
    bi_height = height * 2
    bi_planes = 1
    bi_bitcount = 32
    bi_compression = 0
    bi_size_image = len(xor_bytes) + len(and_mask)
    bi_xppm = 2835
    bi_yppm = 2835
    bi_clr_used = 0
    bi_clr_important = 0

    bmp_info = struct.pack(
        "<IIIHHIIIIII",
        bi_size,
        bi_width,
        bi_height,
        bi_planes,
        bi_bitcount,
        bi_compression,
        bi_size_image,
        bi_xppm,
        bi_yppm,
        bi_clr_used,
        bi_clr_important,
    )

    image_data = bmp_info + xor_bytes + and_mask

    # ICONDIR + ICONDIRENTRY
    header = struct.pack("<HHH", 0, 1, 1)
    width_byte = width if width < 256 else 0
    height_byte = height if height < 256 else 0
    color_count = 0
    reserved = 0
    planes = 1
    bit_count = 32
    bytes_in_res = len(image_data)
    image_offset = 6 + 16

    entry = struct.pack(
        "<BBBBHHII",
        width_byte,
        height_byte,
        color_count,
        reserved,
        planes,
        bit_count,
        bytes_in_res,
        image_offset,
    )

    return header + entry + image_data


def main():
    out_path = Path(__file__).resolve().parents[1] / "assets" / "directional-drilling.ico"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    canvas = build_icon_pixels()
    ico_bytes = encode_ico(canvas)
    out_path.write_bytes(ico_bytes)
    print(str(out_path))


if __name__ == "__main__":
    main()
