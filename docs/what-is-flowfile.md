# What is Flowfile?

Flowfile is an open-source data platform that bundles what is usually bought and wired together separately: a **visual pipeline builder**, a **data catalog**, a **scheduler**, and a **Polars-based Python API** — one `pip install`, running on a laptop or as a team server.

It lives in a specific gap: **after the spreadsheet stops scaling, before a data-engineering team becomes the only option.** The work in that gap is always the same shape — pull data from files, databases, cloud storage, or Kafka streams; clean and combine it; keep the results current; let people query, chart, and build on them. In Flowfile all of it is *reproducible by construction*: the work is saved as re-runnable flows, the results live as versioned tables in the catalog, and schedules keep both fresh without anyone pressing Run.

![Where Flowfile sits: a spectrum from a spreadsheet to a full data platform with an engineering team, with Flowfile in the wide middle bundling the canvas, catalog, scheduler, and Python API — files, databases, cloud storage, and Kafka flowing in.](assets/images/concepts/positioning-spectrum.svg)

The same platform reads as two different products depending on who's looking — so this page forks:

<div class="ff-paths">
<a class="ff-path ff-path-teal" href="what-is-flowfile-plain.html">
<strong>For non-technical readers</strong>
Why "can you run that again?" becomes a re-run rather than a rebuild, and what working this way looks like.
</a>
<a class="ff-path ff-path-purple" href="what-is-flowfile-technical.html">
<strong>For technical readers</strong>
What you no longer build or maintain — secrets, environments, connections, scheduling, lineage — with the mechanism behind each.
</a>
</div>

Ready to start? [Pick the guide that matches the work](users/index.md).
