# yzix

Warning/disclaimer: these programs and libraries are highly experimental and
might (and probably will) break in almost any scenario.

The goal of this project is to combine the power of a content-addressed nix-like
store with some-what incremental and parallel compilation like ccache and distcc.

It is orientied around a client-server model, but the servers itself might also
perform peer-to-peer work-distribution to increase parallelism.
