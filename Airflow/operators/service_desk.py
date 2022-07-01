#!/usr/bin/env python 
# -*- coding: utf-8 -*-

from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable

from urllib.parse import urlencode
import json

ROX_USER = '1340005618'


class OpenServiceDeskTicketOperator(SimpleHttpOperator):
    template_fields = ('error_msg',)

    def __init__(self, subject, error_msg, servicedesk_token_var, username_client, username_creator=None, *args,
                 **kwargs):
        super(OpenServiceDeskTicketOperator, self).__init__(
            endpoint='public/v1/tickets',
            method='POST',
            headers={
                'Content-Type': 'application/json'
            },
            *args,
            **kwargs
        )

        self.subject = subject
        self.token = Variable.get(servicedesk_token_var)
        self.username_client = username_client
        self.username_creator = username_creator
        self.params = {
            'token': self.token,
            'returnAllProperties': False
        }
        self.error_msg = error_msg
        self.message = ''

    def get_person(self, user_name, context):
        previous_endpoint = self.endpoint
        previous_params = self.params
        previos_method = self.method

        self.endpoint = 'public/v1/persons'
        self.method = 'GET'
        self.params.update({
            '$filter': f"userName eq '{user_name}'",
            '$select': 'id'
        })
        self.endpoint += '?{}'.format(urlencode(self.params))
        result = json.loads(super(OpenServiceDeskTicketOperator, self).execute(context))

        self.method = previos_method
        self.params = previous_params
        self.endpoint = previous_endpoint

        return result[0]['id']

    def execute(self, context):
        created_by = self.get_person(self.username_creator, context) if self.username_creator else ROX_USER
        client = self.get_person(self.username_client, context)

        self.message = {
            'type': 2,
            'subject': self.subject,
            'ownerTeam': 'Analytics',
            'category': 'Incidente',
            'urgency': 'Alta',
            'createdBy': {
                'id': created_by
            },
            'clients': [
                {
                    'id': client
                }
            ],
            'actions': [
                {
                    'type': 2,
                    'description': self.error_msg
                }
            ]
        }

        self.endpoint += '?{}'.format(urlencode(self.params))
        self.data = json.dumps(self.message, indent=0)
        super(OpenServiceDeskTicketOperator, self).execute(context)


def notify_servicedesk(
        name,
        http_conn_id,
        subject,
        error_msg,
        servicedesk_token_var,
        username_client,
        username_creator=None
):
    def _internal(context):
        operator = OpenServiceDeskTicketOperator(
            task_id=f'alert_{name}',
            subject=subject,
            error_msg=error_msg,
            http_conn_id=http_conn_id,
            username_client=username_client,
            username_creator=username_creator,
            servicedesk_token_var=servicedesk_token_var
        )

        return operator.execute(context=context)

    return _internal
