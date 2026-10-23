from django import forms
from django.contrib.auth.models import User, Group
from django.contrib.auth.forms import PasswordChangeForm
from control_plane.models import SourceProfile, UserSourceAccess, Role

class UserRegistrationWithAccessForm(forms.ModelForm):
    password = forms.CharField(
        widget=forms.PasswordInput(attrs={
            'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'
        }),
        required=True
    )
    role = forms.ChoiceField(
        choices=Role.choices,
        initial=Role.VIEWER,
        widget=forms.Select(attrs={
            'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'
        })
    )

    class Meta:
        model = User
        fields = ['username', 'email', 'first_name', 'last_name', 'password']
        widgets = {
            'username': forms.TextInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'}),
            'email': forms.EmailInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'}),
            'first_name': forms.TextInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'}),
            'last_name': forms.TextInput(attrs={'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'}),
        }

    def save(self, commit=True):
        user = super().save(commit=False)
        user.set_password(self.cleaned_data['password'])
        if commit:
            user.save()
            # Assign screen-level role group
            role_choice = self.cleaned_data.get('role')
            if role_choice:
                group, _ = Group.objects.get_or_create(name=role_choice)
                user.groups.add(group)
        return user

class UserEditForm(forms.ModelForm):
    first_name = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm',
            'placeholder': 'Enter first name'
        })
    )
    last_name = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm',
            'placeholder': 'Enter last name'
        })
    )
    email = forms.EmailField(
        required=False,
        widget=forms.EmailInput(attrs={
            'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm',
            'placeholder': 'Enter email address'
        })
    )
    role = forms.ChoiceField(
        choices=Role.choices,
        widget=forms.Select(attrs={
            'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'
        })
    )
    new_password = forms.CharField(
        required=False,
        widget=forms.PasswordInput(attrs={
            'class': 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm',
            'placeholder': 'Leave blank to keep current password'
        }),
        help_text="Optional: Enter a new password to reset this user's password."
    )

    class Meta:
        model = User
        fields = ['first_name', 'last_name', 'email']

    def __init__(self, *args, **kwargs):
        user = kwargs.get('instance')
        super().__init__(*args, **kwargs)
        if user and user.groups.exists():
            self.fields['role'].initial = user.groups.first().name

    def save(self, commit=True):
        user = super().save(commit=False)
        
        # Check if a new password was provided for reset
        new_password = self.cleaned_data.get('new_password')
        if new_password:
            user.set_password(new_password)
            
        if commit:
            user.save()
            # Update screen-level role group
            user.groups.clear()
            role_choice = self.cleaned_data.get('role')
            if role_choice:
                group, _ = Group.objects.get_or_create(name=role_choice)
                user.groups.add(group)
        return user

class CustomPasswordChangeForm(PasswordChangeForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        input_classes = 'w-full px-4 py-3 bg-slate-900 border border-slate-700 rounded-xl text-white placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm'
        for field_name in self.fields:
            self.fields[field_name].widget.attrs.update({'class': input_classes})
        
        # Clear out the verbose help text on the new password field
        self.fields['new_password1'].help_text = None