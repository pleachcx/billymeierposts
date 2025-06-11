---
layout: default
title: "Predictions"
---

<ul>
{% assign prediction_pages = site.pages | where_exp: 'page', "page.path contains 'predictions/'" %}
{% for page in prediction_pages %}
  {% unless page.path == 'predictions/index.md' %}
    <li><a href="{{ page.url | relative_url }}">{{ page.name }}</a></li>
  {% endunless %}
{% endfor %}
</ul>
