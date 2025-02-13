import json
import os
import shutil
import unittest
from pathlib import (
    Path,
)

import numpy as np

from dpgen2.conf.unit_cells import (
    generate_unit_cell,
)

from .context import (
    dpgen2,
)


class TestGenerateUnitCell(unittest.TestCase):
    def test_bcc(self):
        sys = generate_unit_cell("bcc", 2.0)
        self.assertAlmostEqual(sys["cells"][0][0][0], 2.0)
        self.assertEqual(sys["atom_numbs"], [2])
        np.testing.assert_array_almost_equal(
            sys["atom_types"], [0] * sum(sys["atom_numbs"])
        )

    def test_fcc(self):
        sys = generate_unit_cell("fcc", 2.0)
        self.assertAlmostEqual(sys["cells"][0][0][0], 2.0)
        self.assertEqual(sys["atom_numbs"], [4])
        np.testing.assert_array_almost_equal(
            sys["atom_types"], [0] * sum(sys["atom_numbs"])
        )

    def test_hcp(self):
        sys = generate_unit_cell("hcp", 2.0)
        self.assertAlmostEqual(sys["cells"][0][0][0], 2.0)
        self.assertEqual(sys["atom_numbs"], [2])
        np.testing.assert_array_almost_equal(
            sys["atom_types"], [0] * sum(sys["atom_numbs"])
        )

    def test_sc(self):
        sys = generate_unit_cell("sc", 2.0)
        self.assertAlmostEqual(sys["cells"][0][0][0], 2.0)
        self.assertEqual(sys["atom_numbs"], [1])
        np.testing.assert_array_almost_equal(
            sys["atom_types"], [0] * sum(sys["atom_numbs"])
        )

    def test_diamond(self):
        sys = generate_unit_cell("diamond", 2.0)
        self.assertAlmostEqual(sys["cells"][0][0][0], 2.0 * np.sqrt(2.0))
        self.assertEqual(sys["atom_numbs"], [2])
        np.testing.assert_array_almost_equal(
            sys["atom_types"], [0] * sum(sys["atom_numbs"])
        )
