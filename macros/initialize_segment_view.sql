{% macro initialize_segment_view(view_name) %}
CREATE SCHEMA IF NOT EXISTS `{{ target.database }}`.molly;
CREATE VIEW IF NOT EXISTS `{{ target.database }}`.molly.{{ view_name }} AS (
    -- These post-hooks are used to "initialize" views for BI to suck less. They are intended to be overwritten by Molly Platform
    SELECT * FROM {{this}}
)
{%  endmacro %}