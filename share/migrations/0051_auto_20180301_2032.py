# -*- coding: utf-8 -*-
# Generated by Django 1.11.4 on 2018-03-01 20:32
from __future__ import unicode_literals

import django.contrib.postgres.fields.jsonb
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('share', '0050_harvest_job_index'),
    ]

    operations = [
        migrations.AddField(
            model_name='sourceconfig',
            name='regulator_steps',
            field=django.contrib.postgres.fields.jsonb.JSONField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='agentidentifier',
            name='host',
            field=models.TextField(blank=True),
        ),
        migrations.AlterField(
            model_name='agentidentifier',
            name='scheme',
            field=models.TextField(blank=True),
        ),
        migrations.AlterField(
            model_name='agentidentifierversion',
            name='host',
            field=models.TextField(blank=True),
        ),
        migrations.AlterField(
            model_name='agentidentifierversion',
            name='scheme',
            field=models.TextField(blank=True),
        ),
        migrations.AlterField(
            model_name='workidentifier',
            name='host',
            field=models.TextField(blank=True),
        ),
        migrations.AlterField(
            model_name='workidentifier',
            name='scheme',
            field=models.TextField(blank=True, help_text='A prefix to URI indicating how the following data should be interpreted.'),
        ),
        migrations.AlterField(
            model_name='workidentifierversion',
            name='host',
            field=models.TextField(blank=True),
        ),
        migrations.AlterField(
            model_name='workidentifierversion',
            name='scheme',
            field=models.TextField(blank=True, help_text='A prefix to URI indicating how the following data should be interpreted.'),
        ),
    ]