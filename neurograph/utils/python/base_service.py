import os
import time
import threading
import traceback
from _socket import gaierror
from typing import Union, List

import json
import configparser
from datetime import datetime
from pika.exceptions import AMQPError

import pika_utils
from state import State


class ConfigurationManager:
    def __init__(self, config_path):
        self.config = configparser.ConfigParser(allow_no_value=True)
        config_files = ['rmq_connection_details', 'common']
        for config_file in config_files:
            config_file_path = config_path + f'/{config_file}.ini'
            print('Reading configuration from', config_file_path)
            self.config.read(config_file_path)

    def get_property_group(self, property_group: str):
        return dict(self.config[property_group])

    def get_property(self, property_group: str, property_name: str):
        return self.config.get(property_group, property_name)


configuration_manager = ConfigurationManager(os.environ['CONF_PATH'])


class RmqProfile:
    GLOBAL = dict(configuration_manager.get_property_group('global_rmq'))
    LOCAL = dict(configuration_manager.get_property_group('local_rmq'))


class RmqInputInfo:
    def __init__(self, queue_name, exchange_name=None, prefetch_count: int = 100, durable=True):
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.prefetch_count = prefetch_count
        self.durable = durable

    def create_channel(self, profile, callback):
        if self.exchange_name is None:
            print(f'Creating input connection to queue {self.queue_name}')
            return pika_utils.Blocking.queue(queue_name=self.queue_name, callback=callback, host=profile['host'],
                                             port=profile['port'], prefetch_count=self.prefetch_count, durable=self.durable)
        else:
            print(f'Creating input connection to exchange {self.exchange_name} through {self.queue_name}')
            queue_name = self.exchange_name + '.' + self.queue_name
            return pika_utils.Blocking.queue(queue_name=queue_name, callback=callback, exchange_name=self.exchange_name,
                                             exchange_type=pika_utils.FANOUT, host=profile['host'], port=profile['port'],
                                             prefetch_count=self.prefetch_count, durable=self.durable)


class RmqOutputInfo:
    def __init__(self, name=None, is_exchange=False, durable=True):
        self.name = name
        self.is_exchange = is_exchange
        self.durable = durable
        self.properties = RmqOutputInfo.__create_publishing_properties(durable)

    @staticmethod
    def __create_publishing_properties(durable: bool):
        import pika
        result = pika.BasicProperties()
        if durable:
            result.delivery_mode = 2
        return result

    def create_channel(self, profile):
        if self.is_exchange:
            print(f'Creating output connection to exchange {self.name}')
            return pika_utils.Blocking.exchange(name=self.name, exchange_type=pika_utils.FANOUT, host=profile['host'],
                                                port=profile['port'], durable=self.durable)
        else:
            print(f'Creating output connection to queue {self.name}')
            return pika_utils.Blocking.queue(queue_name=self.name, host=profile['host'], port=profile['port'],
                                             durable=self.durable)


class BaseService:
    def __init__(self, input: Union[RmqInputInfo, str] = None,
                 outputs: Union[RmqOutputInfo, List[str], List[RmqOutputInfo], str] = None):
        self.flow_name = os.environ.get('FLOW_NAME')
        self.service_name = os.environ.get('SERVICE_NAME')
        self.input = None if input is None else RmqInputInfo(input) if type(input) == str else input
        self.outputs = None if outputs is None \
            else [RmqOutputInfo(outputs)] if type(outputs) == str \
            else [outputs] if type(outputs) == RmqOutputInfo \
            else [RmqOutputInfo(output) if type(output) == str else output for output in outputs]
        self.suspended = True
        self.terminated = False

        # input
        self._connect_to_input_queue()
        # output
        self._connect_to_output_queues()

        self.__connect_to_commands_queue()
        self.hive_commands_thread = threading.Thread(target=self.__listen_commands, daemon=True)
        self.__connect_to_heartbeats_queue()
        self.__connect_to_errors_queue()

        # state
        self.state_path = 'state/' + self.service_name
        self.state = State(self.state_path)
        self._init_state()
        self.state.load()

    def _connect_to_input_queue(self):
        if self.input is not None:
            self.input_channel = self.input.create_channel(RmqProfile.LOCAL, self._handle_message_wrapper)
            print('Successfully set up input connection to local RabbitMQ')

    def _connect_to_output_queues(self):
        if self.outputs is not None:
            self.output_channels = []
            for output in self.outputs:
                self.output_channels.append(output.create_channel(RmqProfile.LOCAL))
                print('Successfully set up output connection to local RabbitMQ')

    def __connect_to_heartbeats_queue(self):
        self.hive_heartbeats_channel = pika_utils.Blocking.queue('heartbeats', host=RmqProfile.GLOBAL['host'],
                                                                 port=RmqProfile.GLOBAL['port'])
        self.hive_heartbeats_thread = threading.Thread(target=self.__send_heartbeats, daemon=True)

    def __connect_to_commands_queue(self):
        self.hive_commands_channel = pika_utils.Blocking.queue(self.flow_name + '.' + self.service_name,
                                                               exchange_name='commands',
                                                               routing_key=self.flow_name + '.' + self.service_name,
                                                               exchange_type=pika_utils.TOPIC,
                                                               callback=self.__handle_command_wrapper,
                                                               host=RmqProfile.GLOBAL['host'],
                                                               port=RmqProfile.GLOBAL['port'])

    def __connect_to_errors_queue(self):
        self.hive_errors_channel = pika_utils.Blocking.queue('errors', host=RmqProfile.GLOBAL['host'],
                                                             port=RmqProfile.GLOBAL['port'])

    def _init_state(self):
        pass

    def _handle_message(self, message):
        raise NotImplementedError()

    def _handle_message_wrapper(self, channel, method, properties, body):
        message = body.decode('utf8')
        channel.basic_ack(delivery_tag=method.delivery_tag)
        with self.state.write_lock:
            self.state._last_received_message_datetime = str(datetime.now())
            self.state._current_message = message
            self.state.dump({'_current_message'})
            try:
                self._handle_message(message)
            except Exception:
                self._report_error(traceback.format_exc(), cause=message)
            self.state._current_message = None
            self.state.dump()

    def _hard_shutdown(self, args):
        self.state.dump()
        self.terminated = True
        self._suspend({})

    def _suspend(self, args):
        self.suspended = True

    def _resume(self, args):
        self.suspended = False

    def _handle_predefined_command(self, command, args):
        if command.lower() == 'shutdown':
            print('Processing shutdown command')
            self._hard_shutdown(args)
        if command.lower() == 'suspend':
            print('Processing suspend command')
            self._suspend(args)
        if command.lower() == 'resume':
            print('Processing resume command')
            self._resume(args)

    def _handle_command(self, command, args):
        self._handle_predefined_command(command, args)

    def __handle_command_wrapper(self, channel, method, properties, body):
        message = json.loads(body)
        with self.state.write_lock:
            command = message.get('command')
            args = [] if message.get('args') is None else message.get('args')
            self._handle_command(command, args)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def _send(self, message):
        while True:
            try:
                for output, output_channel in zip(self.outputs, self.output_channels):
                    if output.is_exchange:
                        output_channel.basic_publish(exchange=output.name, routing_key='', body=message,
                                                     properties=output.properties)
                    else:
                        output_channel.basic_publish(exchange='', routing_key=output.name, body=message,
                                                     properties=output.properties)
                self.state._last_sent_message_datetime = str(datetime.now())
                break
            except AMQPError:
                print('Sending failed due to local RMQ disconnect and is blocked until connection restored. '
                      'Reconnecting in 10 seconds')
                while True:
                    try:
                        time.sleep(10)
                        self._connect_to_output_queues()
                        break
                    except AMQPError:
                        print('Unable to reconnect to output queues. Next attempt in 10 seconds')

    def _run(self):
        self.hive_commands_thread.start()
        self.hive_heartbeats_thread.start()
        self.suspended = False

    def __listen_commands(self):
        while True:
            try:
                self.hive_commands_channel.start_consuming()
            except AMQPError:
                print('Global RabbitMQ connection closed. Commands receiving interrupted. Reconnecting in 10 seconds')
                while True:
                    try:
                        time.sleep(10)
                        self.__connect_to_commands_queue()
                        print('Successfully reconnected to commands queue.')
                        break
                    except (AMQPError, gaierror):
                        print('Unable to reconnect to commands queue. Next attempt in 10 seconds')
            except Exception:
                print(traceback.format_exc())
                time.sleep(0.1)

    def __send_heartbeats(self):
        while True:
            try:
                heartbeat_message = {
                    'pipeline': self.flow_name,
                    'service': self.service_name,
                    'state': 'suspended' if self.suspended else 'ok',
                    'last_heartbeat_datetime': time.time(),
                    'last_received_message_datetime': self.state._last_received_message_datetime,
                    'last_sent_message_datetime': self.state._last_sent_message_datetime}
                self.hive_heartbeats_channel.basic_publish(exchange='', routing_key='heartbeats',
                                                           body=json.dumps(heartbeat_message))
            except AMQPError:
                print('Global RabbitMQ connection closed. Unable to send heartbeat. Reconnecting in 10 seconds')
                while True:
                    try:
                        time.sleep(10)
                        self.__connect_to_heartbeats_queue()
                        break
                    except (AMQPError, gaierror):
                        print('Unable to reconnect to heartbeats queue. Next attempt in 10 seconds')
            except Exception:
                print(traceback.format_exc())
                time.sleep(0.1)
            print('heartbeat sent')
            time.sleep(5)

    def _report_error(self, error_message, cause='Unknown'):
        try:
            print('ERROR', error_message)
            body = {'pipeline': self.flow_name, 'service': self.service_name, 'text': error_message, 'cause': cause,
                    'timestamp': str(datetime.now())}
            self.hive_errors_channel.basic_publish(exchange='', routing_key='errors', body=json.dumps(body))
        except AMQPError:
            print(f'Global RabbitMQ connection closed. Error message can\'t be reported: {error_message}. ' +
                  'Reconnecting in 10 seconds.')
            while True:
                try:
                    time.sleep(10)
                    self.__connect_to_errors_queue()
                    break
                except (AMQPError, gaierror):
                    print('Unable to reconnect to errors queue. Next attempt in 10 seconds')
        except Exception:
            print(traceback.format_exc())
            time.sleep(0.1)


class ServiceUtils:
    @staticmethod
    def start_service(service: BaseService, name: str):
        if name == "__main__":
            try:
                service._run()
            except:
                print('Error occurred during startup:', traceback.format_exc())
