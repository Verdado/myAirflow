from typing import Optional
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.plugins_manager import AirflowPlugin

import logging
import pypd


class PagerDutyIncidentOperator(BaseOperator):
    """
    Creates PagerDuty Incidents.
    """

    @apply_defaults
    def __init__(self,
                 routing_key = 'dummy',
                 dedup_key: Optional[str] = None,
                 summary = 'dummy',
                 severity = 'warning',
                 source = 'dummy',
                 *args,
                 **kwargs):
        super(PagerDutyIncidentOperator, self).__init__(*args, **kwargs)
        self.routing_key = routing_key
        self.summary = summary
        self.severity = severity
        self.source = source
        self.dedup_key = dedup_key

    def execute(self, **kwargs):
        pypd.proxies = {'http': 'http://dev-proxy.db.rakuten.co.jp:9501', 'https': 'https://dev-proxy.db.rakuten.co.jp:9501'}

        data = {'routing_key': self.routing_key, 'event_action': 'trigger', 'payload': {'summary': self.summary, 'severity': self.severity.lower(), 'source': self.source}}

        if self.dedup_key:
            data["dedup_key"] = self.dedup_key

        logging.info(data)

        try:
            pypd.EventV2.create(data=data)

        except Exception as e:
            msg = "PagerDuty API call failed ({})".format(e)
            logging.error(msg)
            raise AirflowException(msg)

