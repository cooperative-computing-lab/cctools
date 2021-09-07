
changequote([,])

define(HEADER,[#] $1(1))dnl
define(SECTION,[##] $1)dnl
define(SUBSECTION,[###] $1)dnl
define(SUBSUBSECTION,[####] $1)dnl

changequote

define(PARA,)dnl
define(LINK,[$1]($2))dnl
define(MANUAL,LINK($1,$2))dnl
define(MANPAGE,LINK($1($2),$1.md))dnl
define(BOLD,**$1**)dnl
define(ITALIC,_$1_)dnl
define(CODE,**$1**)dnl


define(LIST_BEGIN,)
define(LIST_ITEM,- $1)
define(LIST_END,)

define(SPACE, )
define(HALFTAB,    )
define(TAB,HALFTAB()HALFTAB())
define(PARAM,ITALIC(&lt;$1&gt;))

define(OPTIONS_BEGIN,LIST_BEGIN)
define(OPTION_FLAG,- BOLD(-$1)`,'BOLD(--$2)<br />)dnl
define(OPTION_FLAG_SHORT,- BOLD(-$1)<br />)dnl
define(OPTION_FLAG_LONG,- BOLD(--$1)<br />)dnl
define(OPTION_ARG,- BOLD(-$1)`,'BOLD(--$2=PARAM($3))<br />)dnl
define(OPTION_ARG_SHORT,- BOLD(-$1) PARAM($2)<br />)dnl
define(OPTION_ARG_LONG,- BOLD(--$1=PARAM($2))<br />)dnl
define(OPTIONS_END,LIST_END)

define(LONGCODE_BEGIN,changequote([,])[changequote([,])```changequote]changequote)
define(LONGCODE_END,changequote([,])[changequote([,])```changequote]changequote)

define(FOOTER,CCTools CCTOOLS_VERSION)dnl
