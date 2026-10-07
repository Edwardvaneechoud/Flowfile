"""``util/layout/placement.py``: positions for nodes added to a canvas that already holds nodes, groups and comments."""

from flowfile_core.flowfile.util.layout.placement import (
    FALLBACK,
    MAX_ROWS,
    NODE_HEIGHT,
    Y_SPACING,
    Box,
    Placer,
    node_box,
    placer_from_payload,
)


def test_boxes_that_only_touch_do_not_overlap():
    assert not node_box(0, 0).overlaps(node_box(180, 0))
    assert not node_box(0, 0).overlaps(node_box(0, 80))
    assert node_box(0, 0).overlaps(node_box(179, 79))


def test_an_empty_canvas_places_at_the_fallback():
    assert Placer().place(1) == FALLBACK


def test_a_node_goes_one_column_right_of_its_input():
    assert Placer({1: (100, 50)}).place(2, inputs=[1]) == (350, 50)


def test_a_taken_slot_moves_the_node_one_row_down():
    assert Placer({1: (0, 50), 2: (250, 50)}).place(3, inputs=[1]) == (250, 150)


def test_the_row_above_is_used_when_the_row_below_is_taken():
    assert Placer({1: (0, 150), 2: (250, 150), 3: (250, 250)}).place(4, inputs=[1]) == (250, 50)


def test_several_inputs_place_right_of_the_rightmost_at_their_median_height():
    assert Placer({1: (0, 0), 2: (100, 200)}).place(3, inputs=[1, 2]) == (350, 100)


def test_a_source_goes_one_column_left_of_its_output():
    assert Placer({1: (500, 50)}).place(2, outputs=[1]) == (250, 50)


def test_a_disconnected_node_goes_below_the_content():
    assert Placer({1: (0, 50), 2: (250, 300)}).place(3) == (0, 300 + NODE_HEIGHT + Y_SPACING)


def test_a_comment_is_avoided():
    comment = Box(250, 0, 240, 300)
    x, y = Placer({1: (0, 50)}, [comment]).place(2, inputs=[1])
    assert x == 250 and not node_box(x, y).overlaps(comment)


def test_a_collapsed_group_read_from_a_payload_is_avoided():
    payload = {
        "nodes": [{"id": 1, "x_position": 0, "y_position": 50}],
        "groups": [{"id": 1, "x_position": 240, "y_position": 0, "width": 300, "height": 180, "collapsed": True}],
        "comments": [],
    }
    x, y = placer_from_payload(payload).place(2, inputs=[1])
    assert x == 250 and not node_box(x, y).overlaps(Box(240, 0, 300, 180))


def test_an_expanded_group_is_a_container_a_node_may_land_in():
    payload = {
        "nodes": [{"id": 1, "x_position": 0, "y_position": 50}],
        "groups": [{"id": 1, "x_position": -40, "y_position": -20, "width": 600, "height": 300, "collapsed": False}],
        "comments": [],
    }
    assert placer_from_payload(payload).place(2, inputs=[1]) == (250, 50)


def test_a_released_node_frees_its_slot():
    placer = Placer({1: (0, 50), 2: (250, 50)})
    placer.release(2)
    assert placer.place(3, inputs=[1]) == (250, 50)


def test_a_node_released_before_placing_does_not_push_the_band_down():
    placer = Placer({1: (0, 50), 2: (0, 900)})
    placer.release(2)
    assert placer.place(3) == (0, 50 + NODE_HEIGHT + Y_SPACING)


def test_a_free_slot_is_not_recorded():
    placer = Placer({1: (0, 50)})
    assert placer.free_slot(0, 50) == (0, 150)
    assert placer.free_slot(0, 50) == (0, 150)


def test_a_placed_node_is_avoided_and_anchors_the_next():
    placer = Placer({1: (0, 50)})
    assert placer.place(2, inputs=[1]) == (250, 50)
    assert placer.place(3, inputs=[1]) == (250, 150)
    assert placer.place(4, inputs=[3]) == (500, 150)


def test_a_chain_in_one_batch_lines_up():
    placed = Placer({1: (0, 50)}).place_all([2, 3, 4], {2: [1], 3: [2], 4: [3]}, {})
    assert placed == {2: (250, 50), 3: (500, 50), 4: (750, 50)}


def test_a_new_source_feeding_a_join_lands_beside_the_joins_other_input():
    placed = Placer({1: (0, 50)}).place_all([2, 3], {3: [1, 2]}, {2: [3]})
    assert placed == {3: (250, 50), 2: (0, 150)}


def test_a_disconnected_subgraph_starts_a_band_below_the_content():
    placed = Placer({1: (0, 50)}).place_all([2, 3], {3: [2]}, {2: [3]})
    band = 50 + NODE_HEIGHT + Y_SPACING
    assert placed == {2: (0, band), 3: (250, band)}


def test_placement_is_deterministic():
    def run():
        placer = Placer({1: (0, 50), 2: (250, 50), 3: (500, 120)}, [Box(260, 160, 200, 100)])
        return placer.place_all([4, 5, 6, 7], {4: [1], 5: [1], 6: [4, 5], 7: []}, {7: [6]})

    assert run() == run()


def test_a_column_with_no_free_row_falls_back_below_everything():
    wall = Box(250, -(MAX_ROWS + 5) * Y_SPACING, 10, 2 * (MAX_ROWS + 5) * Y_SPACING)
    assert Placer({1: (0, 50)}, [wall]).place(2, inputs=[1]) == (250, wall.bottom + Y_SPACING)
