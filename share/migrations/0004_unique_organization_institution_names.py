# -*- coding: utf-8 -*-
# Generated by Django 1.9.7 on 2016-10-21 13:16
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('share', '0003_create_share_user'),
    ]

    # Meant to be run on an empty database, so don't worry about existing duplicates
    operations = [
        migrations.RunSQL(
            "CREATE UNIQUE INDEX share_agent_unique_institution_organization_names ON share_agent (name) WHERE type in ('share.institution', 'share.organization', 'share.consortium');",
            reverse_sql="DROP INDEX IF EXISTS share_agent_unique_institution_organization_names;"
        )
    ]