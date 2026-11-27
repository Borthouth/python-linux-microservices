from django.contrib.auth.decorators import login_required, user_passes_test
from django.shortcuts import get_object_or_404, render
from django.utils import timezone
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_POST

from control_plane.forms import SourceProfileForm
from control_plane.models import DbChoice, SourceProfile
from control_plane.views.sources import is_admin
from etl.cdc import check_mongodb_cdc_readiness


@login_required
@user_passes_test(is_admin)
@require_POST
@never_cache
def source_mongodb_topology_check_view(request, pk):
    profile = get_object_or_404(SourceProfile, pk=pk, db_type=DbChoice.MONGODB)
    result = check_mongodb_cdc_readiness(profile)
    return render(request, "control_plane/source_edit.html", {
        "form": SourceProfileForm(instance=profile),
        "profile": profile,
        "mongodb_topology_result": result,
        "mongodb_topology_checked_at": timezone.now(),
    })
