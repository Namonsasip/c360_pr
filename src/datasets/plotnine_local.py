import copy
import os
import pathlib
import typing

import plotnine
from kedro.io import AbstractDataSet


class PlotninePNGLocalDataSet(AbstractDataSet):
    """
    This IO class writes png files from a plotnine object.
    """

    DEFAULT_SAVE_ARGS = {}  # type: Dict[str, Any]

    def __init__(self, filepath: str, save_args: typing.Dict[str, any] = {}) -> None:
        """
        Creates a new instance of ``PlotninePNGLocalDataSet`` pointing to a concrete
        filepath.

        Args:
            filepath: path to a png file
            save_args: plotnine options for saving
        """
        self._filepath = filepath
        self._save_args = copy.deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

    def _describe(self) -> typing.Dict[str, typing.Any]:
        return dict(filepath=self._filepath, save_args=self._save_args)

    def _load(self) -> None:
        raise NotImplementedError(f"{self.__class__.__name__} has no ``_load`` method.")

    def _save(self, plotnine_object: plotnine.ggplot) -> None:
        path_parent = pathlib.Path(self._filepath).parent
        if not os.path.exists(path_parent):
            os.makedirs(path_parent)
        plotnine_object.save(self._filepath, **self._save_args)
