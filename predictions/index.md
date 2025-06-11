---
layout: default
title: "Predictions"
---

<ul>
{% assign prediction_files = site.static_files | where_exp: "file", "file.path contains 'predictions/'" %}
{% for file in prediction_files %}
  {% unless file.path == 'predictions/index.md' %}
    <li><a href="{{ file.path | relative_url }}">{{ file.name }}</a></li>
  {% endunless %}
{% endfor %}
</ul>
