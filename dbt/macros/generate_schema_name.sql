{#
  dbt's default generate_schema_name macro CONCATENATES the profile's default
  dataset with any custom +schema config (e.g. "olist_silver_olist_gold").
  We want +schema to set the BigQuery dataset directly instead — this is the
  standard override for that behavior.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
