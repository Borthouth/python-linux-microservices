Performance Watchdog for Django and Celery
===========================================

Overview
--------
Performance Watchdog is a lightweight monitoring component for identifying:

- Slow or database-heavy Django HTTP requests.
- Long-running Celery tasks.
- Celery task completion status.

Django requests are monitored by middleware. When a request exceeds a configured duration or database-query threshold, the middleware sends an incident payload to Celery. Celery then writes a formatted request report to a dedicated performance_watchdog.log file.

Celery tasks are monitored separately using Celery task lifecycle signals. The monitor records the task name, task ID, execution duration and completion status without requiring changes to each individual task.

What it records
---------------

Django HTTP request reports include:

- Request path
- HTTP method
- Total request execution time
- Number of Django database queries executed during the request
- Database host
- Authenticated user ID, or Anonymous
- Up to the five most recent SQL queries captured during the request

Celery task reports include:

- Celery task name
- Celery task ID
- Total execution duration
- Completion status, such as SUCCESS or FAILURE

Important scope
---------------
Performance Watchdog monitors two separate execution paths:

1. Django HTTP request processing.
2. Celery task execution.

The Django middleware measures only the HTTP request lifecycle. If an HTTP request starts a Celery task, the middleware measures the request that submitted the task, not the later task execution.

The Celery signal monitor separately measures the execution of the Celery task itself.

Package layout
--------------
Place the package inside the Django project so it is importable by both the web application and the Celery worker:

BacsActive/
    performance_watchdog/
        __init__.py
        conf.py
        middleware.py
        tasks.py
        celery_monitor.py

The __init__.py file should exist but may be empty.

No entry in INSTALLED_APPS is required. The package has no models, migrations, admin registration or AppConfig requirement.

Installation into an existing Django project
--------------------------------------------

1. Copy the package

Copy the complete performance_watchdog directory into the Django project alongside the existing application packages.

Ensure it is visible on PYTHONPATH inside both the Django/Gunicorn container and the Celery container.

2. Add the Django middleware

Add the following entry to the Django MIDDLEWARE setting:

    'performance_watchdog.middleware.PerformanceWatchdogMiddleware',

For example:

    MIDDLEWARE = [
        ...
        'performance_watchdog.middleware.PerformanceWatchdogMiddleware',
    ]

Do not add performance_watchdog to INSTALLED_APPS.

3. Configure request-monitoring thresholds

Add the following settings to the Django settings file:

    WATCHDOG_ENABLED = True
    WATCHDOG_TIME_THRESHOLD = 1.0
    WATCHDOG_QUERY_COUNT_THRESHOLD = 50

WATCHDOG_TIME_THRESHOLD is measured in seconds.

WATCHDOG_QUERY_COUNT_THRESHOLD is the number of database queries executed during one HTTP request.

A request report is generated when EITHER threshold is exceeded.

Examples using the settings above:

- A request taking 1.5 seconds with 10 queries is reported.
- A request taking 0.4 seconds with 70 queries is reported.
- A request taking 0.3 seconds with 8 queries is ignored.

Thresholds should be adjusted to suit the normal performance characteristics of the application.

4. Register the watchdog reporting task and Celery monitor

The package is deliberately not included in INSTALLED_APPS, so Django/Celery task autodiscovery will not necessarily load it.

Import both modules from the existing Celery application configuration:

    import performance_watchdog.tasks  # noqa: E402, F401
    import performance_watchdog.celery_monitor  # noqa: E402, F401

For BacsActive, add the imports to workflow/celery_control.py after the Celery application has been configured.

Example:

    app.config_from_object('django.conf:settings', namespace='CELERY')
    app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)

    app.conf.update(
        result_expires=timedelta(hours=12),
        broker_transport_options={'heartbeat': 3600},
    )

    import performance_watchdog.tasks  # noqa: E402, F401
    import performance_watchdog.celery_monitor  # noqa: E402, F401

The Celery worker must be restarted after adding or changing either import.

5. Configure the dedicated log

The watchdog components use this logger:

    logging.getLogger('performance_watchdog')

Configure a matching handler and logger in Django's LOGGING configuration.

Example handler:

    'performance_watchdog': {
        'level': 'WARNING',
        'class': 'concurrent_log_handler.ConcurrentRotatingFileHandler',
        'maxBytes': 1024 * 1024 * 50,
        'backupCount': 20,
        'filename': os.path.join(
            LOGGING_DIRECTORY,
            'performance_watchdog.log'
        ),
        'formatter': 'standard',
    },

Example logger:

    'performance_watchdog': {
        'handlers': ['performance_watchdog'],
        'propagate': False,
        'level': 'WARNING',
    },

The intended log file is:

    performance_watchdog.log

The propagate setting should remain False so watchdog reports are not duplicated in the general Celery log.

Docker logging configuration
----------------------------
Some projects replace LOGGING['handlers'] and LOGGING['loggers'] inside a DOCKER-specific settings block.

If the application does this, add the performance_watchdog handler and logger to the Docker-specific LOGGING configuration. Adding them only to the initial LOGGING dictionary is not sufficient if that dictionary is later replaced.

Django request monitoring
-------------------------
The request-monitoring flow is:

    HTTP request
        -> Django
        -> PerformanceWatchdogMiddleware
        -> measure duration and database query count
        -> threshold check
        -> incident payload sent through RabbitMQ/Celery
        -> process_slow_request_incident task
        -> formatted report written to performance_watchdog.log

The reporting task runs asynchronously so formatting and log writing do not need to be performed directly inside the HTTP request.

The middleware ignores requests beginning with:

    /static/
    /media/
    /health/

These exclusions avoid generating incidents for static resources, media resources and health checks.

Example Django request report
-----------------------------

    ======================================================================
    PERFORMANCE WATCHDOG INCIDENT
    ======================================================================

    Request Path: /example/workflow/newtask/profilePermissionReport
    HTTP Method: GET
    Total Execution Duration: 0.0445 seconds
    Total Database Queries: 15
    Remote Database Host: database.example.internal
    User ID: 260

    Recent SQL Queries Executed:
     - SELECT ...
     - INSERT INTO ...
     - UPDATE ...

    ======================================================================

Celery task monitoring
----------------------
Celery tasks are monitored using the task_prerun and task_postrun signals in celery_monitor.py.

The monitor:

- Records a high-resolution start time when a task starts.
- Calculates the task duration when the task finishes.
- Writes the task name, task ID, execution duration and final state to performance_watchdog.log.
- Excludes the performance_watchdog reporting task itself.
- Automatically covers other registered Celery tasks without modifying each task function.

The monitor intentionally excludes tasks whose names begin with:

    performance_watchdog.

This prevents the watchdog from reporting its own internal request-reporting task.

Example Celery task report
--------------------------

    ======================================================================
    CELERY PERFORMANCE WATCHDOG
    ======================================================================

    Task: workflow.tasks.profilePermissionReport
    Task ID: 192da0d1-62cd-4ab1-a202-f08b9aaafedc
    Execution Duration: 0.1447 seconds
    Status: SUCCESS

    ======================================================================


Testing
-------

1. Temporarily set low request thresholds

To report every normal Django request during initial testing:

    WATCHDOG_TIME_THRESHOLD = 0.0
    WATCHDOG_QUERY_COUNT_THRESHOLD = 0

Do not leave zero thresholds enabled in normal operation. Every request will create a report and dispatch a Celery task.

2. Restart the application processes

Restart Django/Gunicorn after middleware or Django settings changes.

Restart Celery after changing tasks.py, celery_monitor.py, the logger configuration or the Celery imports.

Example Docker commands:

    docker restart Gunicorn-0
    docker restart Celery-0

3. Verify imports

From the Gunicorn environment:

    python3.11 -c "from performance_watchdog.middleware import PerformanceWatchdogMiddleware; print('Middleware OK')"

From the Celery environment:

    python3.11 -c "import performance_watchdog.celery_monitor; print('Celery monitor OK')"

4. Run an HTTP request

Use the application normally or make a direct test request to Gunicorn.

Confirm that a PERFORMANCE WATCHDOG INCIDENT appears in performance_watchdog.log.

5. Run a normal Celery task

Start an existing task such as workflow.tasks.profilePermissionReport through the application.

Confirm that a CELERY PERFORMANCE WATCHDOG report appears in performance_watchdog.log with the task name, ID, duration and status.

6. Follow the dedicated log

    tail -f /home/accountis/BacsHub/logs/docker/performance_watchdog.log

SQL capture
-----------
The middleware includes up to the last five SQL queries from a qualifying HTTP request.

Large SQL statements can make reports difficult to read and can produce unnecessarily large Celery messages. A recommended implementation is to truncate each captured SQL statement, for example:

    incident_data['recent_queries'] = [
        query.get('sql', '')[:1000]
        for query in recent_queries[-5:]
    ]

Celery thresholds
-----------------
The current celery_monitor.py reports every monitored Celery task so that normal task durations can first be observed.

After collecting representative timings, a future enhancement can introduce a separate setting such as:

    WATCHDOG_CELERY_TIME_THRESHOLD = 5.0

The post-run handler could then write a report only when a task exceeds that threshold or finishes unsuccessfully.

This threshold is not part of the current basic implementation unless it has been added separately.

Security and data considerations
--------------------------------
The request report can contain application paths, user identifiers, database host names and SQL statements. Depending on the application, SQL may contain or reveal sensitive application data.

Treat performance_watchdog.log according to the same access, retention and security requirements as other application diagnostic logs.

Do not add request bodies, passwords, authentication tokens, private keys or other secrets to the incident payload.

Operational notes
-----------------
- The request watchdog depends on the existing Celery/RabbitMQ infrastructure for asynchronous reporting.
- The Celery monitor uses in-process task lifecycle signals and does not create another Celery job.
- If Celery reports an unregistered performance_watchdog task, verify that performance_watchdog.tasks is explicitly imported and restart the worker.
- If Celery task reports do not appear, verify that performance_watchdog.celery_monitor is explicitly imported by the Celery application and restart the worker.
- If reports appear in celery.0.log instead of performance_watchdog.log, verify the dedicated logger and handler and confirm propagate is False.
- In Docker, verify that performance_watchdog is visible inside both Gunicorn and Celery containers.
- Fast workflow polling endpoints can generate many request reports when request thresholds are set to zero. Restore sensible thresholds after testing.
- The task state shown in a Celery report comes from Celery's task_postrun signal.

Current capabilities
--------------------
Performance Watchdog helps answer:

- Which Django requests are slow?
- Which Django requests execute many database queries?
- What recent SQL was associated with a qualifying request?
- Which Celery task ran?
- How long did the Celery task take?
- What final state did Celery report for the task?

It provides diagnostic evidence. It does not automatically optimise SQL, change application code, determine root cause or restart failed services.
