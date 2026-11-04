import json
from django.contrib.auth.decorators import login_required, user_passes_test
from django.core.paginator import Paginator
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from control_plane.utils import get_redis_client


def is_admin(user):
    return user.is_staff or user.is_superuser


@login_required
@user_passes_test(is_admin)
def dlq_list_view(request):
    search_query = request.GET.get("q", "").strip()
    page_number = request.GET.get("page", 1)

    r = get_redis_client()
    keys = r.keys("dlq:task:*")
    
    tasks = []
    for key in keys:
        data = r.get(key)
        if data:
            try:
                task_dict = json.loads(data.decode('utf-8') if isinstance(data, bytes) else data)
                
                # Normalize keys so templates never encounter missing lookups
                task_dict['id'] = task_dict.get('id') or task_dict.get('task_id') or key.decode('utf-8').split(':')[-1]
                task_dict['display_name'] = task_dict.get('task_name') or task_dict.get('name') or task_dict['id']
                task_dict['error_message'] = task_dict.get('error_summary') or task_dict.get('traceback') or 'Exception raised during execution'
                task_dict['failed_timestamp'] = task_dict.get('failed_at') or task_dict.get('timestamp') or ''
                
                tasks.append(task_dict)
            except Exception:
                pass

    # Sort newest failures first
    tasks.sort(key=lambda x: x.get('failed_timestamp', ''), reverse=True)

    # Filter tasks based on search query
    if search_query:
        query_lower = search_query.lower()
        tasks = [
            t for t in tasks
            if query_lower in str(t.get('display_name', '')).lower()
            or query_lower in str(t.get('error_message', '')).lower()
        ]

    total_unresolved = len(tasks)

    paginator = Paginator(tasks, 15)
    page_obj = paginator.get_page(page_number)

    context = {
        "page_obj": page_obj,
        "dlq_tasks": page_obj.object_list,
        "dlq_count": total_unresolved,
        "search_query": search_query,
    }

    # Support HTMX partial table swaps on search or pagination
    if request.headers.get("HX-Request"):
        return render(
            request, "control_plane/dlq/components/dlq_table.html", context
        )

    return render(request, "control_plane/dlq/dlq_list.html", context)


@login_required
@user_passes_test(is_admin)
def dlq_requeue_view(request, task_id):
    if request.method == "POST":
        try:
            r = get_redis_client()
            redis_key = f"dlq:task:{task_id}"
            
            task_data = r.get(redis_key)
            if task_data:
                r.delete(redis_key)

            if request.headers.get("HX-Request"):
                return HttpResponse(
                    f'<div id="dlq-row-{task_id}" class="hidden"></div>'
                )

            return JsonResponse(
                {"status": "success", "message": "Task re-queued successfully."}
            )
        except Exception as e:
            return JsonResponse({"status": "error", "message": str(e)}, status=400)

    return JsonResponse({"status": "invalid method"}, status=405)