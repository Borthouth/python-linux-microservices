from django.core.management.base import BaseCommand
from django_celery_beat.models import PeriodicTask, IntervalSchedule
import json

class Command(BaseCommand):
    help = 'Sets up default periodic tasks for schema introspection and drift detection.'

    def handle(self, *args, **options):
        # Create a schedule to run every 1 hour (adjust as needed)
        schedule, created = IntervalSchedule.objects.get_or_create(
            every=1,
            period=IntervalSchedule.HOURS,
        )

        # Note: If you want it per-source, you can pass arguments like [source_pk]
        # For a global periodic trigger that loops through active sources, you can create a wrapper task.
        task_name = 'Introspect Source Schemas (Hourly)'
        
        periodic_task, created = PeriodicTask.objects.get_or_create(
            task='etl.tasks.introspect_source_schema_task',
            defaults={
                'interval': schedule,
                'name': task_name,
                'queue': 'queue_periodic',
                # If your task requires an argument (like a specific source_id), 
                # you can pass args here as a JSON string: json.dumps([source_id])
                'args': json.dumps([]), 
            }
        )

        if created:
            self.stdout.write(self.style.SUCCESS(f"Successfully created periodic task: {task_name}"))
        else:
            self.stdout.write(self.style.WARNING(f"Periodic task '{task_name}' already exists."))