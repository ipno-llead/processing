from typing import Tuple

import numpy as np


def _y_min_x_min(o):
    return o["geometry"][0][1], o["geometry"][0][0]


def _x_min(o):
    return o["geometry"][0][0]


def _overlap_y(a, b):
    ya_min, ya_max = a["geometry"][0][1], a["geometry"][1][1]
    yb_min, yb_max = b["geometry"][0][1], b["geometry"][1][1]
    return (ya_min >= yb_min and ya_min <= yb_max) or (
        yb_min >= ya_min and yb_min <= ya_max
    )


def _overlap_x(a, b):
    xa_min, xa_max = a["geometry"][0][0], a["geometry"][1][0]
    xb_min, xb_max = b["geometry"][0][0], b["geometry"][1][0]
    return (xa_min >= xb_min and xa_min <= xb_max) or (
        xb_min >= xa_min and xb_min <= xa_max
    )


def _line_y_dist_within_range(a, b, line_y_dist_range) -> bool:
    if a["geometry"][0][1] > b["geometry"][0][1]:
        a, b = b, a
    d = b["geometry"][0][1] - a["geometry"][1][1]
    min, max = line_y_dist_range
    return d > min and d < max


def _line_height(o):
    g = o["geometry"]
    return g[1][1] - g[0][1]


def _line_height_is_similar(a, b):
    lh_a, lh_b = _line_height(a), _line_height(b)
    return lh_b > lh_a * 0.66 and lh_b < lh_a * 1.5


def _mean_n_std(l) -> Tuple[float, float]:
    # arr = np.array(l)
    mean = np.mean(l, axis=0)
    sd = np.std(l, axis=0)
    return mean, sd


def _valid_line_y_dist_range(data):
    """returns valid range of line distance of lines within a block

    basically returns (mean - std, mean + std)
    """
    dists = []
    for blk in data["blocks"]:
        prev_line = None
        for line in sorted(blk["lines"], key=_y_min_x_min):
            if prev_line is not None:
                dists.append(line["geometry"][0][1] - prev_line["geometry"][1][1])
            prev_line = line
    # dists_arr = np.array(dists)
    mean, sd = _mean_n_std(dists)
    return mean - sd, mean + sd


def _rearrange_lines_into_blocks(data, line_y_dist_range):
    """Rearranges lines into blocks based on the following heuristic:

    Lines that satisfy the following conditions are grouped into the same block:
    - lines that are within line_y_dist_range from each other
    - overlap in the x dimension
    - line height must not be too big or too small compared to the previous line
    """
    lines = sorted(
        [line for blk in data["blocks"] for line in blk["lines"]], key=_y_min_x_min
    )
    blocks = list()
    for line in lines:
        for blk in blocks:
            if (
                _overlap_x(blk["lines"][-1], line)
                and _line_y_dist_within_range(blk["lines"][-1], line, line_y_dist_range)
                and _line_height_is_similar(blk["lines"][-1], line)
            ):
                blk["lines"].append(line)
                break
        else:
            blocks.append({"lines": [line]})
    # calculate block geometry
    for blk in blocks:
        geometries = [l["geometry"] for l in blk["lines"]]
        blk["geometry"] = [
            [
                min(g[0][0] for g in geometries),
                min(g[0][1] for g in geometries),
            ],
            [
                max(g[1][0] for g in geometries),
                max(g[1][1] for g in geometries),
            ],
        ]
    return blocks


def _rearrange_blocks_into_paragraphs(blocks):
    paragraphs = list()
    p_blocks = list()
    prev_blk = None
    for blk in sorted(blocks, key=_y_min_x_min):
        if prev_blk is None or (prev_blk is not None and _overlap_y(prev_blk, blk)):
            p_blocks.append(blk)
        else:
            paragraphs.append(p_blocks)
            p_blocks = [blk]
        prev_blk = blk
    paragraphs.append(p_blocks)
    return paragraphs


def _concat_text(paragraphs):
    return [
        [
            [
                " ".join([word["value"] for word in line["words"]])
                for line in blk["lines"]
            ]
            for blk in sorted(blocks, key=_x_min)
        ]
        for blocks in paragraphs
    ]


def relayout_doc():
    """read ocr results for a single document and concat into paragraphs for each page"""
    line_y_dist_range = None

    def relayout_page(page):
        nonlocal line_y_dist_range
        if line_y_dist_range is None:
            line_y_dist_range = _valid_line_y_dist_range(page)
            # print(f"line_y_dist_range: {line_y_dist_range}")
        blocks = _rearrange_lines_into_blocks(page, line_y_dist_range)
        paragraphs = _rearrange_blocks_into_paragraphs(blocks)
        return _concat_text(paragraphs)

    return relayout_page
