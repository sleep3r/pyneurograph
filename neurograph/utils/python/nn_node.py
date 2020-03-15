from base_service import *


class NNNode(BaseService):
    def _suspend(self, args):
        if self.suspended:
            return
        super()._suspend(args)
        self.input_channel.stop_consuming()
        # if args.get('dump_state'): # TODO add parameters and then uncomment
        self.state.dump()

    def _resume(self, args):
        if not self.suspended:
            return
        self.state.load()
        super()._resume(args)

    def __handle_rmq_disconnect(self):
        print('RabbitMQ connection closed. Reconnecting in 10 seconds')
        while True:
            try:
                time.sleep(10)
                self._connect_to_input_queue()
                break
            except AMQPError:
                print('Unable to reconnect. Next attempt in 10 seconds')

    def __handle_processing_error(self):
        print('Exception during message processing:', traceback.format_exc())
        self._report_error(traceback.format_exc())
        time.sleep(0.1)

    def _process_unfinished_task(self):
        if self.state._current_message is not None:
            print('There is unfinished task in the state. Processing started')
            try:
                self._handle_message(self.state._current_message)
            except AMQPError:
                self.__handle_rmq_disconnect()
            except Exception:
                self.__handle_processing_error()

    def _run(self):
        super()._run()
        self._process_unfinished_task()
        while True:
            try:
                while self.suspended:
                    if self.terminated:
                        print('Shutting down')
                        return
                self.input_channel.start_consuming()
            except AMQPError:
                self.__handle_rmq_disconnect()
            except Exception:
                self.__handle_processing_error()
