class BreadcrumbMixin:
    """
    A reusable mixin for class-based views to inject dynamic breadcrumbs 
    into the template context.
    """
    breadcrumbs = []

    def get_breadcrumbs(self):
        # Can be overridden in a view for dynamic parameters (e.g. object names)
        return self.breadcrumbs

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['breadcrumbs'] = self.get_breadcrumbs()
        return context