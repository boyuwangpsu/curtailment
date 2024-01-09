# First-party imports
from gluonts.core.component import validated
from gluonts.model.estimator import Estimator

# Relative imports
from ._predictor import CrostonForecastPredictor


class CrostonForecastEstimator(Estimator):
    @validated(
        getattr(CrostonForecastPredictor.__init__, "Model")
    )  # Reuse the model Predictor model
    def __init__(self, **kwargs) -> None:
        super().__init__(predictor_cls=CrostonForecastPredictor, **kwargs)