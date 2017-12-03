[33mcommit 66a11e1f5633dd4745ae451069b0ca2589be9656[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Dec 3 22:07:33 2017 +0000

    adding success col to Record db
    
    Committer: Graham Hukill <ghukill@gmail.com>

[33mcommit 260f12be8d629851094d8d67351a4c39c6956b5f[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Dec 1 20:15:31 2017 +0000

    es indexing for large jobs success

[33mcommit f1aefd8a107911b223c1a2d2be35722212adee00[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Dec 1 13:44:51 2017 +0000

    linking from ES search to record details

[33mcommit 96921a45726a574f5556e7fcc101e75d72049dfb[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 30 21:57:28 2017 +0000

    reverting back

[33mcommit 12802fec914adcabf5d19878ad8c5f9a77c72407[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 30 20:05:29 2017 +0000

    splitting up ES index

[33mcommit 9219f4d2f79bef516b8dcb0d2b3dd6e3c7766614[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 29 19:44:44 2017 +0000

    work on large jobs

[33mcommit 533c7c2aa847902dd9946222f35a44bc5e445598[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 29 15:50:59 2017 +0000

    large merge testing

[33mcommit 7034897fbc0d95b783ea739f85c53871268b05aa[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 28 20:06:30 2017 +0000

    tuning with partitioning

[33mcommit 49950ba75d6601c1f69bf30ccf559b777dc6a088[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 28 14:09:38 2017 +0000

    fix import error with ES

[33mcommit 8bcac46da218eeb21ab897c95e06f1a29a5b1f0e[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 28 13:51:06 2017 +0000

    fix to records DT table for publish

[33mcommit dcdd69db5f6456f0e4d24315d5fd1e7c833740ea[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 28 12:36:54 2017 +0000

    exclude agg match, still agg field exists

[33mcommit e9b2a6a2a005e865f916e15815a29d3e7a201358[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 28 12:31:03 2017 +0000

    setting spark max workers default 1

[33mcommit 7ab54dade94add8624a3fc9ce15ac704057137cd[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 28 02:43:38 2017 +0000

    partitioning optimization

[33mcommit 43f563d7a9bb2a53875ef720c591845de2292def[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Nov 27 23:47:48 2017 +0000

    repartitioning begin

[33mcommit 77ddc3fc66fce3cb88a3cb0cf4b9e28555f1e79f[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Nov 27 20:17:47 2017 +0000

    sorting for values per field dataframe

[33mcommit 30bc2b0c84c16da7582b382159508dfb7fb21e46[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Nov 27 18:08:48 2017 +0000

    rough counts coming through

[33mcommit bd5cd035ededa9bbd8619843f75ab172053552f5[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Nov 27 17:53:58 2017 +0000

    with/without for field match

[33mcommit 0065f11ab5f26c756ba3d597347407f558a46aaf[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Nov 27 17:13:41 2017 +0000

    field analysis progress

[33mcommit ff71a836ebde0648a35572aa63d2dc6383ec5baf[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Nov 27 14:55:56 2017 +0000

    removing ESDT table from job details

[33mcommit e37ab6c182c7bcd6adee4c5d7627ed96132b1450[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Nov 27 14:12:51 2017 +0000

    work on field analysis

[33mcommit 1327a2e19331ab4dd008b47d31279573c9b51813[m
Merge: aeb63b2 084a8f2
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Nov 27 07:17:53 2017 -0500

    Merge pull request #55 from WSULib/esdt
    
    Esdt

[33mcommit aeb63b22ff1204e92c7a5fca7b0e33edafd307e2[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sat Nov 25 23:31:13 2017 +0000

    fixing dependencies

[33mcommit fd706069f9ce56d85045b8a6909cdc613a5cda31[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sat Nov 25 22:58:55 2017 +0000

    removing cyavro/cython

[33mcommit 084a8f28754972a71740f4ae43d36dd2a2adf8c3[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 22 20:27:43 2017 +0000

    timing DTElasticSearch calculation

[33mcommit bb3a33d6ec31fca2594c19ee03c4b714c8b923fd[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 22 20:15:37 2017 +0000

    pre-tbreak

[33mcommit 22eb5fbde4bd154b7514e995795595ca704bb31c[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 22 20:09:29 2017 +0000

    preliminary DT for field analysis

[33mcommit 0fe5371cbd3da920ab1bbf939a0a5d999e78c4c2[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 22 19:26:13 2017 +0000

    transitioning field analysis to dt

[33mcommit fbaf6ade6e1f736546753a1fa6025b56d6ae9b07[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 22 18:29:23 2017 +0000

    move DTElasticSearch to models

[33mcommit 07a029a5e7b89fd5eefa8a902d0e8a9fd588f0ae[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 22 17:58:52 2017 +0000

    quiet logging from runserver

[33mcommit cd8b450dc29ec3756537dbcb6dea80679297d2a3[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 22 17:27:21 2017 +0000

    rough es fields table in job details

[33mcommit 3576b6c0cea71f14ea1792ae419d7ef28a28752c[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 22 17:21:03 2017 +0000

    progress on es dt

[33mcommit db38be887bf2ea8d239ef0cdac59293ef2060664[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 22 16:29:58 2017 +0000

    ES DataTables connector

[33mcommit e5bd79b8ceb749a561f8d3f1e1712e2b7d07b468[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 22 14:36:17 2017 +0000

    adding combine_db_id to es index

[33mcommit a8931a7ce911e342db628be1007dff33a63c7420[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 22 14:05:18 2017 +0000

    xslt processor for es

[33mcommit 349ee9f314211af4356c7e4523c00afeac1480d6[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 21 20:19:44 2017 +0000

    dpla mapping contd.

[33mcommit 9a442ade0471b60cd453aa833ddee0a0bec8096c[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 21 19:30:05 2017 +0000

    rough mapping from indexed fields

[33mcommit ed1a4e38077d6370ab6922129c12c160e37aa419[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 21 17:55:20 2017 +0000

    commenting out thumbs in records DT

[33mcommit 706f7eed7ef4e6060c0aa0caf797fa72ec093827[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 21 17:39:26 2017 +0000

    DPLA mapping for record page

[33mcommit 43f00ec44d07a24567c1671f3b456ddf3a3066aa[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 21 14:21:44 2017 +0000

    show DPLA maps in record view

[33mcommit 542d9aaf9226b14da1369905fa8f98c017c4b617[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 21 14:08:37 2017 +0000

    thumb proof of concept

[33mcommit f47f77315faea39d224292a555d3b66f7f8e80c8[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 21 13:56:42 2017 +0000

    show mapped field in job details

[33mcommit 2d567f8848f234ff779155f960dc323f60bafcd2[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 21 13:12:37 2017 +0000

    return mappings

[33mcommit ed9c97affd941fc6e2ae9425373613ae7f3f3052[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 21 12:49:19 2017 +0000

    experimental DPLA job mapping

[33mcommit 06b2e01dc8de874809c63a121e2a16907d1498af[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Nov 20 20:27:30 2017 +0000

    formatting

[33mcommit cb3d8c2edfb57e3a67936f8494a42070d3905737[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Nov 20 19:33:44 2017 +0000

    documenting field analysis

[33mcommit 0c16795362ad564d039134dba230f38e475154f1[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Nov 20 19:27:49 2017 +0000

    read from DB for ES and avro, field analysis

[33mcommit 150c31a21c41fd4d28a6a9ed18ed902535e1e773[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Nov 20 13:46:21 2017 +0000

    highlight one, distinct per doc

[33mcommit 2237a779c0b91f4bb8e01909a49d0ddc5f324922[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Nov 20 12:59:22 2017 +0000

    adding total vs. distinct vals of field

[33mcommit a41a7a2b436fd666ce8c223b10e8d5cb3120538e[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Nov 20 12:24:03 2017 +0000

    updating conda and pip

[33mcommit 15a166f25bfc2d521e158f296584a6626f3c7551[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Nov 17 20:07:00 2017 +0000

    updating docs model

[33mcommit b0e7ec5537599bf5d311865d947f6246ca01dcc7[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Nov 17 20:06:34 2017 +0000

    links to XML from DT

[33mcommit 515d976b456cee864cc6e121f0f50c698b96bbcc[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Nov 17 20:00:54 2017 +0000

    abstracted indexed fields analysis

[33mcommit 9a34c5de21fde1af7ea59b43eafa4aa970cc36f2[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Nov 17 19:08:54 2017 +0000

    small fix to generic mapper

[33mcommit ac3e96d4447123803099ec4bec6a7f203ec2101f[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Nov 17 16:47:51 2017 +0000

    publish n jobs from record group

[33mcommit a2808c55661b143bf7ec74c3454de9e7e19f1e5f[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Nov 17 14:56:08 2017 +0000

    DT for published records

[33mcommit cc23b5b592c8f9c6b0aff79cceb632a6e57aae55[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Nov 17 14:18:14 2017 +0000

    refactor es analysis to ESIndex

[33mcommit 05a806cb88a0e85837521b7ba427574f424f8035[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Nov 17 13:03:02 2017 +0000

    elapsed fix

[33mcommit 4174a54a0e7dd18666cb67f00fc427adee04d731[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Nov 17 12:45:57 2017 +0000

    reworking job timing for threadsafety

[33mcommit 47aa0cc9522855662970875abff7cdc48a300add[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 16 20:21:11 2017 +0000

    job work

[33mcommit f5ed3de52dbdf45d7900abb05ae7c571bbc636f7[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 16 17:30:18 2017 +0000

    pre-optimize

[33mcommit 988d7ca2a3f6aefee62e2d20502331e00fd152ba[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 16 17:15:33 2017 +0000

    fix to field analysis es query

[33mcommit bb762bf498f2b65cfdba4d4800e60511b764e8ad[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 16 16:23:55 2017 +0000

    cautiously hopefuly elapsed works

[33mcommit de966e35e9e209a9df1f9baf8ab1e9105b486b8e[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 16 16:15:14 2017 +0000

    work on job elapsed

[33mcommit b980b7c96672fec8296f9c2159b44da19a4f1bc1[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 16 14:44:31 2017 +0000

    redirects

[33mcommit 8c914de5f737be29aa139b83ff55b8ef3b065306[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 16 14:43:00 2017 +0000

    aligning MODSMapper with DBfirst

[33mcommit 0700daa528963bb03dc2b7813e9b7245b2b1dfd9[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 16 12:33:12 2017 +0000

    fixing field analysis links

[33mcommit 3010f4b3108b5734f7f87b4a3d87d7bf4b05bc02[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 15 21:21:52 2017 +0000

    client-side DT for field analysis

[33mcommit 197c9ee9fd3362b8961df9f96c3f9d4d63d08ea8[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 15 20:15:20 2017 +0000

    all jobs page

[33mcommit ac924afa35fd3e22edb65bc8aad6ed7ca1b5b071[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 15 19:03:34 2017 +0000

    vis cleanup

[33mcommit 3d00ff268a30e1f299235f22cd33f162a3c7a635[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 15 18:59:32 2017 +0000

    converting main job table to DT

[33mcommit 63e4493a9ed1e351a71578028af8d87c8817026f[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 15 18:50:39 2017 +0000

    edit job notes from details

[33mcommit 869d0b6613bd04078e4651be36447dbcf8e7eaf9[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 15 18:33:23 2017 +0000

    adding job notes

[33mcommit 5f4326abcf952401d21f9109581ae66e4219635c[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 15 18:13:55 2017 +0000

    4 partitions for avro output

[33mcommit 38ba5ec196705856e05a465945add78d8fd132c5[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 15 18:06:39 2017 +0000

    cleanup pre-merge to dev

[33mcommit 9b9633cab53ec19aeaef7229ca1efc9aab628743[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 15 15:26:35 2017 +0000

    sorting and pre publish

[33mcommit e31cbbf4d5eb0b73fcadfadce9b4ce12eef67228[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 15 15:23:49 2017 +0000

    datatable for field analysis

[33mcommit 7f604e3cc5edd16fe0f52a3a5fe07744841af804[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 15 15:16:17 2017 +0000

    merging appears to work

[33mcommit 9c937fcddbeb1a907492dce1a8ceb8b89e40d74d[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 15 14:58:03 2017 +0000

    pre merge rework

[33mcommit f1447777ca399dc07c69befa8d201ac3f9755e28[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 15 14:47:34 2017 +0000

    fixing indexing for DBfirst

[33mcommit dacce99d5ad0a039fe9b9165bf8e53f6d28c8f78[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 15 14:27:54 2017 +0000

    hours of headache == typo

[33mcommit 39279fd1d5ea1feea6ed59c1745cab70ac5d3b0b[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 15 01:56:06 2017 +0000

    contd.

[33mcommit 935a0d290790dbc4a7661ac314dacd1aa34fca3f[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 14 21:27:00 2017 +0000

    still working on transforms

[33mcommit 545de4e24ea597ca3de60ac057c06cc5745e39a6[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 14 21:02:47 2017 +0000

    working on transform input

[33mcommit f40582d378ab93ea649d1b7ae0eec962193dafa1[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 14 19:13:25 2017 +0000

    removing 'index' col from Record

[33mcommit 46fe5550ef301efda1b5fb1ffa4fb5efabac17ae[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 14 19:00:04 2017 +0000

    filtering out tombstones, grabbing only meta

[33mcommit 385cec8ed33cc6f839f28df40d2d114f12ef17af[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 14 18:37:43 2017 +0000

    progress on Harvest

[33mcommit c3f488a297ca61c4a278f5f0d27c04d8d6c69593[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 14 17:48:00 2017 +0000

    fix indent

[33mcommit 63f73067486a63434514a4db84568b44a098c315[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 14 17:44:38 2017 +0000

    skipping oai set for now, prep for rework

[33mcommit 39696406f665dc3de91e97246a80a8bbd3a78f17[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 14 15:57:44 2017 +0000

    skipping OAI tombstones

[33mcommit a35cfd1a4b9a1ce5775adab5417eb1183ac4d99c[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 14 15:16:42 2017 +0000

    adding oai set to record db

[33mcommit 829e2f45885102bf85e741a6f754e37a711210f3[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 14 13:56:17 2017 +0000

    selecting metadata ele child

[33mcommit c96b3e94cfac71ec0e3052f3e165848afa77a423[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Nov 13 18:55:48 2017 +0000

    removing lxml and spark-xml-utils approaches

[33mcommit 5442211dab5ceada9fa5c8d8f0cc253370d0c973[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Nov 13 18:55:13 2017 +0000

    commit before other transforms

[33mcommit 83f4b6af7cd959392bc723873306b26f2a07e2da[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Nov 13 17:47:23 2017 +0000

    encoding XML where necc

[33mcommit 6a3808fc83d2cfdb35e771c65f43de23fe2da2ad[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Nov 13 17:42:17 2017 +0000

    moving towards pyjxslt

[33mcommit 718e587938dba262c9eb1e4afe06e1721cb1aaf5[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Nov 13 16:58:53 2017 +0000

    experimenting with pyjxslt

[33mcommit b0bbd15387e90ccb77dc6d15d73302b5b7bcc245[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Nov 10 21:27:39 2017 +0000

    exploring use of spark-xml-utils

[33mcommit e378e3bce1c93146c29118f5abf01ee66711f9c7[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Nov 10 13:21:44 2017 +0000

    flag duplicates in DB indexing

[33mcommit 8d69ae57933e1133ec91e04ea8d60d4a12e76265[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 9 21:29:45 2017 +0000

    pre unique

[33mcommit ef3ebc230d6ce30113a0ea9c7f5d7169c28d644a[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 9 20:52:33 2017 +0000

    building out unique field for Record

[33mcommit b8740932b75b82f9761cec0582501d7880def3db[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 9 16:55:09 2017 +0000

    vis

[33mcommit a79f6f1dcfabb38db46f5147edf28b4523627f7d[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 9 16:19:16 2017 +0000

    multivalued fields for ES

[33mcommit c208707f9ea67890d9d97ad9008eac62d2d4f9a4[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 8 21:24:28 2017 +0000

    generic mapper for dc working

[33mcommit 01f5036e008ad4f2901c2661407c4d4615cfbedc[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 8 18:17:05 2017 +0000

    work on generic XML mapper for indexing

[33mcommit c7ae9349b2a15b8a760899762dd3e5b45695026e[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Nov 7 23:21:01 2017 +0000

    xsl work

[33mcommit 2526efbb03020b8732888b143291187e2dfae10e[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Nov 3 12:35:08 2017 +0000

    allow all hosts

[33mcommit 19cbdb4ca6eff7896e0cc46489bfe3c8d6a294f8[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 2 17:28:10 2017 +0000

    small fixes

[33mcommit a03af64713294aae01b41f2a92737e31c01f785c[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 2 17:23:49 2017 +0000

    minor visual changes

[33mcommit 5990c2e452aebe3a3038b8a782c39e8bf152718e[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 2 17:21:55 2017 +0000

    scaffolding for job types

[33mcommit 8ee0c46f8b17f3ccaf256e0acc4dde8b3d869fee[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 2 17:16:24 2017 +0000

    exp. dpla identifier

[33mcommit d6388c4ca010b482bf7016f19111aaf0aedd4434[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 2 16:42:41 2017 +0000

    cursory explanations for job details

[33mcommit e2444feac72ffa60dfbde7a6f4120a1fe95bc767[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Nov 2 14:54:27 2017 +0000

    job details

[33mcommit ca58c922b61b755a2995da9b882190bbac19f4f6[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 1 20:42:23 2017 +0000

    adding db model diagram

[33mcommit 42467f8d66e30cd332e912eabd038f0c9debb0cd[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 1 20:37:19 2017 +0000

    code format contd.

[33mcommit 59d7703bac0b76798db2224bd6de7399c6b060bc[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 1 20:07:09 2017 +0000

    finished with models formatting

[33mcommit 28394a8df150138da20ff8c4bad0ad6224655b12[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Nov 1 19:53:23 2017 +0000

    code format contd.

[33mcommit 5423fab542ea619a5700032b41639c4645bbb8c3[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Oct 31 20:25:51 2017 +0000

    contd. to models 1299

[33mcommit 2398e604726487b178bbfeaf41888b2a736751c8[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Oct 31 20:00:00 2017 +0000

    formatting and commenting

[33mcommit 22823e9e692c55fa02af90cfc641ed4b836e2648[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Oct 31 19:49:55 2017 +0000

    formatting and commenting

[33mcommit a4ddfe32d2fbe7cd542d1d277f1faaa5ec768fd5[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Oct 31 16:38:59 2017 +0000

    formatting contd.

[33mcommit 04f5692a0455ac2d531fa3592d4c01c38edf8a3e[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Oct 31 13:23:39 2017 +0000

    formatting/commenting

[33mcommit 5d880093dd2e1fef094b768fd7319a3f7f65dd5e[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 30 20:50:45 2017 +0000

    writing oai identifier on all jobs to sql

[33mcommit 1b8e47efea05f12e1779789de2eecf7ad69f1a6d[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 30 19:20:53 2017 +0000

    optiona, dynamic OAI identifier method

[33mcommit 1cc1c8c2830cd44e769a0a1c8a4fd8ac6ed90b9a[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 30 17:26:43 2017 +0000

    performance for indexing failures

[33mcommit 3946507373face69fddfd1eaeea7fb671548777f[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 30 16:43:03 2017 +0000

    datatables for indexing failures

[33mcommit 54fb446ff1779abe8742a869e0d710373494df2f[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 30 12:50:11 2017 +0000

    showing input XML for transform job

[33mcommit c912b71aaffb3e93d6a0b88003b52ee1eb5e1544[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Oct 24 14:07:31 2017 +0000

    handle jobs without details

[33mcommit db26e10ff46b5d8a123d746e2d574b4c1b51ab88[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Oct 24 13:57:24 2017 +0000

    showing transformation used

[33mcommit b0bfcf3bf26970ed12a5b8666ef8c2614bbe8c52[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Oct 24 13:11:32 2017 +0000

    adding bootstrap, but not using

[33mcommit 122dc359e7bcf77b7da9ccbfb0815a68a464e7ff[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Oct 24 12:49:46 2017 +0000

    using record stages

[33mcommit 371cc2105245285b2b0791112ccaa3187e3c7891[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 23 20:20:52 2017 +0000

    record ancestry

[33mcommit 25300e3363b3298d3ce627e8e8e7f7191b892cfc[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Oct 22 22:35:47 2017 +0000

    see docs for field count

[33mcommit 1cf204360c54c299b8bd012aa17436ec251c3063[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Oct 22 22:22:32 2017 +0000

    view ES doc

[33mcommit 917c5be84abd0166ff438cf283b42a7550c78568[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Oct 22 21:37:25 2017 +0000

    building our record page

[33mcommit 8f08102863fc8e5874cfbeac0ad038cec4e28f85[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Oct 22 14:46:53 2017 -0400

    adding django-datatables-view

[33mcommit 200cf09cbe3bfaf6b80ddae30d676d13c52482e0[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Oct 22 14:07:15 2017 -0400

    removing cyavro

[33mcommit 3139a85bc9c23301833f007ab1fb3e8e5785098c[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Oct 22 13:31:52 2017 -0400

    removing cyavro

[33mcommit b03c00da99dd885f91b2b7ff7ce4993b23631422[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Oct 22 13:31:32 2017 -0400

    removing cyavro

[33mcommit cbc60579032023f83427df31ff651f0c4dce2862[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Oct 22 13:30:59 2017 -0400

    temporarily skipping install of cyavro

[33mcommit 95576b0765b5f4810b3c9be260d0ab0ce4d50934[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Oct 20 13:44:07 2017 +0000

    exp. xml validation

[33mcommit 181ae01989766e568ce31228467e939d54cdbd29[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Oct 20 13:28:25 2017 +0000

    cleanup

[33mcommit db889aca49d74281f11e73faee95cfff7acd0efb[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Oct 20 13:25:35 2017 +0000

    filtering by record_id, plans for more

[33mcommit b3d14e8951412ab410e554a93d3c387965e39f2a[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Oct 20 13:14:36 2017 +0000

    rough scaffold for DT view

[33mcommit c43e082895cd44c2d932547259b7c3a6f24910db[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Oct 20 13:08:10 2017 +0000

    reworking to use django-datatables-view

[33mcommit b944430a308159d36927b2a9d905531d450dba3b[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 19 15:06:23 2017 +0000

    work on datatables

[33mcommit 46e3cbe72e7cd56b549e896dc57f6b8b240f873b[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 19 12:51:42 2017 +0000

    pagination

[33mcommit 6fb210b3908db9af5ea4a289d8a9680b9d2fff0d[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 19 12:40:42 2017 +0000

    datatables scaffold

[33mcommit 17ba1b4259e80d4bd4c95794a80609a5bc707235[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Oct 18 20:29:14 2017 +0000

    pre record tables and details

[33mcommit 82b2f05968c41fe01c9e9c72658289c9ee8c7a02[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Oct 18 20:18:08 2017 +0000

    scaffolding for record views

[33mcommit 82a43b345e9b4bb59d30490a2d489cf13858b498[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Oct 18 19:59:15 2017 +0000

    additional field metrics

[33mcommit a912ad881bde4e68a25a4018ee2be0fa3297bc58[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Oct 18 18:35:32 2017 +0000

    adding index default to config

[33mcommit d5308fc12fc17c77ccd0eaea6a6d9575571fdb42[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Oct 17 20:33:51 2017 +0000

    mem tuning

[33mcommit 66abe45ce7054628d7cfc57042f855bd55971681[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Oct 17 14:38:59 2017 +0000

    all cyavro moved to DB interactions

[33mcommit 44618c1786180ae78b60d5fabb9af4a5bff69d21[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Oct 17 13:18:24 2017 +0000

    combine oai server pulling from DB

[33mcommit 72f8937e5a4b128c523254602e71f650faecc8ca[m
Merge: 9517dfe 75861df
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 16 20:33:30 2017 +0000

    Merge branch 'dev' of https://github.com/WSULib/combine into dev

[33mcommit 9517dfe28e249884b6f567306487fdf01fb57109[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 16 20:32:43 2017 +0000

    testing FK vs MUL key for Record

[33mcommit 75861df7cc9257a15ac5dd314fe2f78bda680de7[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 16 14:14:11 2017 -0400

    default APP_HOST set to IP

[33mcommit 8f278c778c470e6ce5fa293fc8d24a64c3e31627[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 16 10:59:38 2017 -0400

    updating spark_code field

[33mcommit ddf86a1f22182c40857755c90d14bed0b1670b84[m
Merge: acd06fa 85b3a19
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 16 10:20:16 2017 -0400

    Merge pull request #18 from WSULib/dbrework
    
    merging dbrework for vm build

[33mcommit 85b3a198017cf123917f7a98c6ce4d3a6ebdd2c5[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 16 01:03:06 2017 +0000

    updating requirements

[33mcommit 8efa0de516fe825646bdc625de49a144858fb318[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 16 00:59:34 2017 +0000

    pre index to ES from DB

[33mcommit 8919d8d316baeb53893dad2cc5a285f907c8d360[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 16 00:17:29 2017 +0000

    updating settings

[33mcommit c91747de990b7362757105941e36f31399c48b5d[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 16 00:14:10 2017 +0000

    groundwork for writing job records to DB

[33mcommit acd06faac344fc4f413cda52532c2b4f4fa74aab[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Oct 15 04:09:18 2017 +0000

    removing jupyter tests

[33mcommit fc47642492f9b6cb51179d793f2b98503eed5dee[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Oct 15 04:07:11 2017 +0000

    moving into leveraging DB

[33mcommit 7c7a1418453f6d1ae7c15dc1fa3a965e3f180f72[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Oct 15 03:02:22 2017 +0000

    confirm avro file write

[33mcommit da13cca7704a60424835a56bb4267105d6b104a5[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Oct 15 02:46:24 2017 +0000

    working to index records to DB

[33mcommit b0368f5d92357c5a48b581416f4ef2040fc0e997[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Oct 15 00:58:47 2017 +0000

    fixing indexing

[33mcommit 07782c38f8b006ead856587c4ecbb7402410514f[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Oct 15 00:24:39 2017 +0000

    simplify indexing

[33mcommit 093670a618b45cc677e732c01bd384068eebec45[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sat Oct 14 23:41:58 2017 +0000

    refactoring spark jobs

[33mcommit d6679e4fbb7dc4664f5837932d728e7348eac4f8[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Oct 13 20:29:59 2017 +0000

    job indexing work

[33mcommit 743e213437ff3e1d31094532a742ae06c39b28cb[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Oct 13 18:30:42 2017 +0000

    moving towards DB indexing of jobs

[33mcommit 197f61de0a0460b7c5027e195a22dcbcc388de4e[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Oct 13 14:21:41 2017 +0000

    exploring job output reading

[33mcommit 32f1a74b7b5f74abd4fbfcddd54c0c3b53cc3843[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 12 20:30:15 2017 +0000

    oai and memory tuning

[33mcommit 1b2d33607c792886fa54c63110379a522355f9ca[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 12 19:07:20 2017 +0000

    logging

[33mcommit cdbbfccfb20761bf3b73de653ad2452390caf3c3[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 12 18:58:05 2017 +0000

    fix token race conditions

[33mcommit 4640edc03b797bbfe41a4ee045163251e3b11025[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 12 18:10:31 2017 +0000

    sets in OAI serverE

[33mcommit 84181c5f1105d6eae427188b12c819dee2f69a9b[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 12 17:27:14 2017 +0000

    work on oai server

[33mcommit 04709f9356b6c7cb280ef189a35662c4bcc32d09[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 12 15:27:11 2017 +0000

    pre resumption token

[33mcommit 389e76ed4f805e346c09a7c4c65ffb1e3404063c[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 12 15:05:00 2017 +0000

    rough oai working

[33mcommit 0aa4b11b7bd76542cf44873a3bc66f112566561f[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 12 14:10:21 2017 +0000

    publishing bugs

[33mcommit c527e5d386846c4059a550cf2fa6beb37d78cbb1[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Oct 11 22:59:06 2017 +0000

    fixes

[33mcommit a45f6113e68914b6daaa455470349c367a900a56[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Oct 11 22:57:17 2017 +0000

    pre record retrieval

[33mcommit 6118d56fc9fa618911efddc213385a5c1d5ffe91[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Oct 11 21:11:41 2017 +0000

    prepped for oai responses

[33mcommit 67097d415738ea4d3b3b259239f7edcc7f622f50[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Oct 11 20:58:18 2017 +0000

    ready for OAI tethering

[33mcommit b78b973d2ee6b313d52539986f3357836ebdec34[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Oct 11 20:52:18 2017 +0000

    rough published page

[33mcommit fc0db9e169c0d2a56bc55efa36ae1769e9e92eaf[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Oct 11 19:06:56 2017 +0000

    adding publish_set_id to ES

[33mcommit 3482a4a789263cbf7da1ec64c32f7ffc6deb1f87[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Oct 11 14:38:54 2017 +0000

    reworking publishing

[33mcommit 1596a7e60d90559be3ecca625a45fa761208e4d3[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Oct 10 13:26:14 2017 +0000

    prep for pub agg

[33mcommit fb77ba3b183f8a03f2c9a10d88bb09577173e145[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 9 18:32:27 2017 +0000

    OAI server scaffolding

[33mcommit 2d2721946416b8e196bacb2d6b8d95ba4f39dfbd[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 9 16:12:32 2017 +0000

    refactored index mapper to es.py

[33mcommit fc4521c5bbb3e6716d5e365d2deee4b4ef160d5d[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 9 15:07:18 2017 +0000

    aligning settings

[33mcommit cfb0938abd5c409f2929f77501a3b0462badf0cc[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 9 15:06:29 2017 +0000

    pre-loading python files

[33mcommit 6c9bd5b34002af8ccc2d8c6c5b55a99d7c03dc09[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 9 14:37:18 2017 +0000

    moved to import code

[33mcommit 71003a6718ba512182b255f94a415f533468534a[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 9 13:54:59 2017 +0000

    moving towards importing pyspark code

[33mcommit bdce3b8b95340bbe8bc4a6549d914fdf17db73ae[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Oct 6 20:20:04 2017 +0000

    preparing for use of mappers

[33mcommit f4e9a0fba5c59dc0522b68eb6bfca39163dda66a[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Oct 6 18:39:25 2017 +0000

    done with visual for now

[33mcommit ecd4fa69c5c2153424b089044605675c074a0ab8[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Oct 6 18:31:59 2017 +0000

    bg cleanup

[33mcommit 588144001ec8106a095c17b6c66e007abf4b794a[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Oct 6 18:28:33 2017 +0000

    hacky breadcrumbs

[33mcommit 640cf6f6b077aabf0de12f865304692103a43c6f[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Oct 6 16:52:55 2017 +0000

    visual job select

[33mcommit aa94d19790849414ca43dc2740dc920d84ce1bec[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Oct 6 16:50:03 2017 +0000

    building out config

[33mcommit 4e43b92e0efeca44dd2c1029ad7d72feb25ed7e0[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Oct 6 16:36:00 2017 +0000

    basic CRUD

[33mcommit 1784caf4f9cbf407f729251a8865b93100c50fd9[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Oct 6 16:21:40 2017 +0000

    reworking URLs

[33mcommit 327b0c32873f0375874af833f9ed59b562c67656[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Oct 6 14:46:54 2017 +0000

    merge jobs

[33mcommit 927b0c8fb6ad4de6c82686ce24c3d59636462b3e[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 5 20:27:31 2017 +0000

    page padding

[33mcommit 7b3e068b16f68e6a2adf7354bee4624122d21884[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 5 20:24:30 2017 +0000

    input clarification

[33mcommit 7b6da835fe5e362a57ea5da9744ee6afe7da02de[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 5 20:22:35 2017 +0000

    adding a splash of color

[33mcommit dada4b40bf3f7191fe4532fcd078cee220b6d0c9[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 5 19:55:38 2017 +0000

    pushing publish work

[33mcommit e16dfa039cef840e0bd95bd80dd95874c3a1f22b[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 5 19:49:43 2017 +0000

    cleanup

[33mcommit 4aa44b845468299e40b82183182f0e1e93d64042[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 5 19:49:22 2017 +0000

    work on publishing links

[33mcommit bf6c0d378ee9b32990f8bdd85d639ec88e44dee6[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 5 17:52:25 2017 +0000

    reworking to provide job names

[33mcommit 69003c5b29c1aa765d654db868733934864ccba1[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 5 15:18:37 2017 +0000

    handle no failures

[33mcommit 4fbfdc5c12ecbbabbce9c698ab470ef0bea88e32[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 5 15:16:06 2017 +0000

    reporting job errors

[33mcommit e2d31e67289a21cc33a342809ea2f2c3dbd7efaf[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Oct 5 13:55:31 2017 +0000

    building out job details

[33mcommit 273ba588797356e53aee58b47b64d7caa18bf4ab[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Oct 4 20:26:19 2017 +0000

    updates for failures

[33mcommit d8de8d0e635a25c930fd186be3bb467e833b24b3[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Oct 4 19:20:41 2017 +0000

    moving to EShadoop indexing

[33mcommit 61c04a0441c8337f90678e516538920fecf2ac50[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Oct 3 18:57:18 2017 +0000

    push before record views

[33mcommit 1f9bbf68c12e960d692977a2849d16793f9b61a2[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Oct 3 16:19:36 2017 +0000

    auto indexing post harvest and transform

[33mcommit e5242ca39692092ff38b1763c864c63885fc1eac[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 2 18:08:28 2017 +0000

    dependencies

[33mcommit c52a6ab0b7f9c1114d61203d55b67c70f0f992f2[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Oct 2 18:07:24 2017 +0000

    rough sketches of field analysis

[33mcommit f6691d7c44b3062601f2ba2b7d14e4c631131302[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Oct 1 21:38:21 2017 +0000

    working on indexing

[33mcommit 69df795f69db122a5af87700fb626a1ee09314c2[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Sep 29 20:24:19 2017 +0000

    exploring indexing

[33mcommit 40fb3e45f5fca56ea6881947b1dd06981f7641ea[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Sep 28 17:10:16 2017 +0000

    removing Record for now

[33mcommit f37786c89f0e60a9aae0dd8b333a007ce0f23c11[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Sep 28 17:01:54 2017 +0000

    error handling

[33mcommit 2c4f93527359b7d986ca2234893b118ce3d3d6a8[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Sep 28 13:38:56 2017 +0000

    modeling one-to-many rels for jobs

[33mcommit 178334884f889fca5298ad228ff06ac66b8983b0[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Sep 27 15:43:57 2017 +0000

    pre transformation payload to disk

[33mcommit b8d1407ba99eac1ee729e081e5dfedb5fbdaab5e[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Sep 27 14:13:44 2017 +0000

    job linkage

[33mcommit 5385cbf1f51e653c97a9b40d05ea303e70e8d179[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Sep 27 13:58:47 2017 +0000

    building out input job selection

[33mcommit 55f028d8c8d8328e7e08f9ab15f2a191151343d0[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Sep 27 03:08:58 2017 +0000

    rough transform

[33mcommit 445b3115c2187cb25a0b66085b367bd0dabd0b7a[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Sep 26 20:53:01 2017 +0000

    adding to requirements.txt

[33mcommit 0adf96030b951a7efb1399014d29bf57b7724ea5[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Sep 26 20:50:35 2017 +0000

    transform job

[33mcommit 835d00c7f39c0c2bea1bda1a291e9f56ab5532ac[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Sep 26 14:26:27 2017 +0000

    push before python 3.5

[33mcommit c451ba15c1dfeb37f1062265f90e579ab00d3563[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Sep 25 18:05:10 2017 +0000

    OAI harvesting work

[33mcommit 64df5f2449e7edb7d14ebac69ae5e2b22ed96f63[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Sep 22 16:54:42 2017 +0000

    new counting method for record structure

[33mcommit 9af0f4103fe36ec1bda07ed794cc415c47443f77[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Sep 22 13:20:27 2017 +0000

    tolerance with job deletion

[33mcommit 4b5957cdbe90f44d8cd924c036adfb7843d1c1ab[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Sep 21 13:55:02 2017 +0000

    pyspark shell and prep for record writing

[33mcommit b0f2128b245c331e3e0557728dddaf33d61728ba[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Sep 21 13:18:41 2017 +0000

    remove job_output

[33mcommit 0ae8d1df796f00f6e5d732cb190305b311777d6a[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Sep 20 22:28:26 2017 +0000

    refactoring job submission

[33mcommit e4de65931f19a6100e91e42ef0b6a3693e0657c9[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Sep 19 19:52:32 2017 +0000

    pushing pre Stur

[33mcommit aef5c349ca287e8ca44a2b1f6f65ad8329118ffa[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Sep 19 19:41:57 2017 +0000

    orgs structure made

[33mcommit b36350dee6b396f8b99782540c9a2bdfd9baa18c[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Sep 19 19:34:05 2017 +0000

    adding orgs and updating record groups

[33mcommit 015c83e2b8150bd005e6b8096cfc31813662da88[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Sep 19 16:21:30 2017 +0000

    adding APP_HOST to localsettings

[33mcommit 241f90716c56d0b2d72b625a78ffc1e16eca7eff[m
Merge: 8ef411d ed2f467
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Sep 19 16:19:43 2017 +0000

    Merge branch 'onesession' into dev

[33mcommit ed2f46795a1efd32ee6fdb3470c946e0de622f1a[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Sep 19 16:19:31 2017 +0000

    work on settings

[33mcommit 8ef411dcfebedca55d0ae390e534b512f2b52038[m
Merge: 6aef178 8aa0228
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Sep 19 11:55:38 2017 -0400

    Merge pull request #5 from WSULib/onesession
    
    Using single Livy session

[33mcommit 8aa0228ec3b27336be54ce5131d2b2355dd16ff0[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Sep 19 15:53:07 2017 +0000

    job counting moved to models

[33mcommit ca8a193e7db19742a50632a354b70ffbdf007244[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Sep 18 19:58:15 2017 +0000

    prepping for rework and model move

[33mcommit 2b4ae8ee14a740569c67256c86b0bcc0261e0b00[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Sep 18 14:15:24 2017 +0000

    prepped for cyavro record counting

[33mcommit 3ed7b9c05e6cf627056206cb6e8089cebebd4a5c[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Sep 18 13:14:38 2017 +0000

    confirm active livy session

[33mcommit 7b8338d9ffe425bfa6aceb57654188236eb264a4[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Sep 17 22:28:55 2017 +0000

    pre single session sniffing

[33mcommit f1f8c2680e60149147ba325729000a336c0c0d34[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Sep 17 22:24:35 2017 +0000

    pre single session jobs

[33mcommit 6ea0342474548e5b70e72d129f641448365685b6[m
Merge: 3505a77 17431e3
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Sep 17 22:12:00 2017 +0000

    merging

[33mcommit 3505a77d62115b8f6c9300fcf9ed9caa8729e486[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Sep 17 22:10:32 2017 +0000

    moving towards a single session

[33mcommit 91eb82400526492482f2a0d1d752e422fc47277e[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Sep 17 21:29:59 2017 +0000

    updating for vm defaults

[33mcommit 17431e3b34bcc74931fd048bdf943f455391dfd0[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Sep 15 20:31:03 2017 +0000

    fix for scaffold

[33mcommit 36e2d006289da14b5cebba7532e5f0afc79a06a1[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Sep 15 20:30:06 2017 +0000

    moving towards one session

[33mcommit 6aef178ceae6a9c6aa0848925d22ddd1feb4749e[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Sep 14 08:59:48 2017 -0400

    commiting before sjs branch

[33mcommit 9996438f00e7d8021d5b5aa13aa04c68017f18b0[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Sep 12 13:53:39 2017 -0400

    job init and harvests

[33mcommit 0daad243931d160c2835235db932ef9e31b410fc[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Sep 12 12:41:01 2017 -0400

    job monitoring

[33mcommit 3b70af42024824c1ce4af6ea8cdbc094690f8170[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Sep 12 12:22:14 2017 -0400

    work on counting records

[33mcommit c1793383f66296ef19b04a081e2bdb3f1880cb7f[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Sep 12 11:04:00 2017 -0400

    rough sketch of Job factories

[33mcommit 41230556dffbda7df632a88ed746a333b4ec914e[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Sep 11 16:23:24 2017 -0400

    moving away from livy HttpClient

[33mcommit ad18e708d62fe6929fa3f36ddd30646c353ae63c[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Mon Sep 11 09:49:37 2017 -0400

    preparing for job submissions

[33mcommit 67802a7204fdc101f0ecf00a5fb3853816f49e9c[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Sep 10 20:07:21 2017 -0400

    building out record groups

[33mcommit 82e087d86bb46951739b916129f3452f711383b9[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Sun Sep 10 08:17:09 2017 -0400

    delete livy session

[33mcommit 3260135f0b207f71fd93aa49ca1898818e94b206[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Sep 8 16:24:50 2017 -0400

    clarifying language

[33mcommit b1295ca9dc2beed62acd67b9b438bbbad826241c[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Sep 8 16:00:40 2017 -0400

    fixing typo

[33mcommit 9d7f4551465042929f90a4a7f1e5c7cbff217d02[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Sep 8 15:58:08 2017 -0400

    rough sketch of major pages

[33mcommit f538c86fc18c49b2d147fc3096b5d8aceef8f773[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Sep 8 15:21:52 2017 -0400

    livy session organization

[33mcommit a48d15ea4dde363a9936759528ebe26f309092f5[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Fri Sep 8 10:46:25 2017 -0400

    moving to user sessions

[33mcommit 6aaf40bdfeb156c46d1c53aa2037f3b573b652e7[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Sep 5 15:38:44 2017 -0400

    rough sketch of jobs to record group

[33mcommit 62d153c206a50df5015e10829d4e2860916c9d9f[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Sep 5 15:26:29 2017 -0400

    working on record groups

[33mcommit 489f9b69b1a6cb93857a73de53ea79633f1384a9[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Sep 5 15:08:22 2017 -0400

    adding OAI endpoint to editable admin

[33mcommit cdca031a6fef5186440569016373aaf3b904586c[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Tue Sep 5 14:43:38 2017 -0400

    building out Livy sessions routes

[33mcommit 2169dd2b0496cb664735e0ab206bac0e3b8fe25f[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Aug 31 16:25:44 2017 -0400

    for le day

[33mcommit b0cc25a016462c08dfa5a04ddea09e8f9ddad2ae[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Aug 31 16:23:06 2017 -0400

    localsettings

[33mcommit 37e36ceb62e2e5d919b1e9bb1d62570775c0f519[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Aug 31 16:21:32 2017 -0400

    adding framework for localsettings

[33mcommit 1e21075177f24c6aca02619ee1fc96124c7877fe[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Aug 31 15:32:28 2017 -0400

    livy/spark session creation successful

[33mcommit 9866bb16d28961f0e479c17e18eec1fb5d6296a6[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Aug 31 13:24:30 2017 -0400

    rough scaffolding

[33mcommit 33075f51698e18ed4332ce049f95dc696e9ba2dc[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Aug 31 12:03:31 2017 -0400

    removing south from requirements

[33mcommit 2a4dad7072c6256bddeebd14a96214a6567a147f[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Thu Aug 31 11:49:43 2017 -0400

    first commie

[33mcommit 5a6c213347ceedfc20c6b9e0c234a00feacd9df5[m
Merge: 4e3440c 3ac38f5
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Aug 30 15:39:03 2017 -0400

    Merge pull request #1 from WSULib/add-license-1
    
    adding license

[33mcommit 3ac38f519534e0c0601a19ac4cbb8a199c1d4551[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Aug 30 15:38:49 2017 -0400

    adding license

[33mcommit 4e3440c6454d9ac3878e9691510a5c16bb65371e[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Aug 30 15:34:56 2017 -0400

    Update README.md

[33mcommit c9a146f4080bc317fa61e7e6dbf68f73f1b9ecde[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Aug 30 15:32:57 2017 -0400

    adding tractor

[33mcommit 999e18fa655d9a617870bd6f61cc1e07a6ad2660[m
Author: Graham Hukill <ghukill@gmail.com>
Date:   Wed Aug 30 15:31:24 2017 -0400

    Initial commit
