"""Component entry point for keboola.ex-azure-service-bus."""

import logging
import sys

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

logger = logging.getLogger(__name__)


class Component(ComponentBase):
    """Extractor for Azure Service Bus queues, subscriptions and dead-letter queues."""

    def __init__(self):
        super().__init__()

    def run(self):
        """Main execution code."""
        raise NotImplementedError


if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception:
        logger.exception("Component failed with an unexpected error")
        sys.exit(2)
