define(HEADER,.TH "$1" 1 "CCTOOLS_RELEASE_DATE" "CCTools CCTOOLS_VERSION" "Cooperative Computing Tools")dnl
define(SECTION,.SH $1
.LP)dnl
define(SUBSECTION,.SS $1
.LP)dnl
define(PARA,.PP)dnl
define(BOLD,\fB$1\fP)dnl
define(ITALIC,\fI$1\fP)dnl
define(CODE,\FC$1\FT)dnl
define(LINK,$1 (BOLD($2)))dnl
define(MANUAL,BOLD($1))dnl
define(MANPAGE,BOLD($1($2)))dnl
define(LIST_BEGIN)dnl
define(LIST_ITEM,`.IP \(bu 4'
.
)dnl
define(LIST_END)dnl
define(PARAM,<$1>)dnl
define(OPTIONS_BEGIN,.LP)dnl
define(OPTION_ITEM,.TP
.B \$1
.
)dnl
define(OPTION_PAIR,.TP
.BI \$1 \ PARAM($2)
.
)dnl
define(OPTION_TRIPLET,.TP
BOLD($1)`,' BOLD(-\-$2)`='ITALIC(PARAM($3))
.
)dnl
define(OPTIONS_END)dnl
define(LONGCODE_BEGIN,.fam C
.nf
.nh
.IP "" 8)dnl
define(LONGCODE_END,.fi
.hy
.fam
.P)dnl
define(FOOTER)dnl
dnl
