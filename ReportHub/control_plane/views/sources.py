from django.contrib.auth.decorators import login_required
from django.shortcuts import render, get_object_or_404, redirect
from django.core.paginator import Paginator
from control_plane.models import SourceProfile
from control_plane.forms import SourceProfileForm

@login_required
def source_list_view(request):
    sources_qs = SourceProfile.objects.all().order_by('name')
    
    paginator = Paginator(sources_qs, 15) # 15 items per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    context = {
        'page_obj': page_obj,
    }
    return render(request, 'control_plane/source_list.html', context)

@login_required
def source_create_view(request):
    if request.method == 'POST':
        form = SourceProfileForm(request.POST)
        if form.is_valid():
            form.save()
            return redirect('control_plane:source_list')
    else:
        form = SourceProfileForm()
    return render(request, 'control_plane/source_form.html', {'form': form})

@login_required
def source_edit_view(request, pk):
    profile = get_object_or_404(SourceProfile, pk=pk)
    if request.method == 'POST':
        form = SourceProfileForm(request.POST, instance=profile)
        if form.is_valid():
            form.save()
            return redirect('control_plane:source_list')
    else:
        form = SourceProfileForm(instance=profile)
    
    return render(request, 'control_plane/source_form.html', {'form': form, 'profile': profile})

@login_required
def source_delete_view(request, pk):
    profile = get_object_or_404(SourceProfile, pk=pk)
    if request.method == 'POST':
        profile.delete()
        return redirect('control_plane:source_list')
    
    return render(request, 'control_plane/source_confirm_delete.html', {'profile': profile})

@login_required
def source_toggle_active_view(request, pk):
    profile = get_object_or_404(SourceProfile, pk=pk)
    profile.is_active = not profile.is_active
    profile.save()
    return redirect('control_plane:source_list')