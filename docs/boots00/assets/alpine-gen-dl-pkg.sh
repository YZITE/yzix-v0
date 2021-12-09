# USGAE: ROOTFS=/path/to/alpine ./agdlpkg.sh APKINDEX_UNPACKED PKG_NAME

[ $# -eq 2 ] || exit 1

grep -C 

{
  "nodes": [
    {
      "name": "alpine-reduce-rq",
      "kind": {
        "type": "fetch",
        "hash": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
      },
      "logtag": 0,
      "rest": null
    },
    {
      "name": "alpine-require",
      "kind": {
        "type": "require",
        "hash": [155, 206, 14, 23, 19, 159, 145, 89, 231, 23, 186, 38, 215, 155, 218, 246, 141, 102, 128, 87, 22, 15, 84, 198, 31, 110, 153, 133, 50, 207, 187, 40]
      },
      "logtag": 0,
      "rest": null
    },
    {
      "name": "alpine-reduce",
      "kind": {
        "type": "run",
        "command": [
          [
            {
              "String": "/bin/busybox"
            }
          ],
          [
            {
              "String": "sh"
            }
          ],
          [
            {
              "Placeholder": "reduce"
            }
          ]
        ],
        "envs": {}
      },
      "logtag": 3,
      "rest": null
    }
  ],
  "node_holes": [],
  "edge_property": "directed",
  "edges": [
    [
      2,
      1,
      "Root"
    ],
    [
      2,
      0,
      {
        "Placeholder": "reduce"
      }
    ]
  ]
}
