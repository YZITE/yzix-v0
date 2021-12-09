
curl -O https://bouncer.gentoo.org/fetch/root/all/releases/amd64/autobuilds/latest-stage3-amd64-musl.txt

STAGE3_URL="$(< latest-stage3-amd64-musl.txt)"

cat > graph.json <<EOF
{
  "nodes": [
    {
      "name": "gentoo-stage3.tar.xz",
      "kind": {
        "type": "fetch",
        "url": "$"
      },
      "logtag": 1,
    }
    {
      "name": "alpine-reduce-rq",
      "kind": {
        "type": "require",
        "hash": [105, 46, 37, 95, 23, 233, 10, 101, 31, 164, 226, 200, 222, 182, 201, 48, 45, 37, 99, 32, 63, 210, 226, 3, 192, 110, 112, 197, 85, 118, 152, 113]
      },
      "logtag": 0,
      "rest": null
    },
    {
      "name": "alpine-APKINDEX.tar.gz",
      "kind": {
        "type": "fetch",
        "url": "https://dl-5.alpinelinux.org/alpine/latest-stable/main/x86_64/APKINDEX.tar.gz",
        "hash": null,
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
      "logtag": 1,
      "rest": null
    },
    {
      "name": "alpine-dl",
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
      "logtag": 1,
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
EOF
