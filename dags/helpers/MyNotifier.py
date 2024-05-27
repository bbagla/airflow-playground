from airflow.notifications.basenotifier import BaseNotifier

class MyNotifier(BaseNotifier):
    template_fields = ("message",)

    def __init__(self, message):
        self.message = message

    def notify(self, context):
        title = f"Task {context['task_instance'].task_id} "
        #mocking
        print( "Mock: ", title, self.message)