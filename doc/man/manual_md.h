
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
define(PARAM,<$1>)

define(OPTIONS_BEGIN,LIST_BEGIN)
define(OPTION_ITEM,LIST_ITEM(BOLD($1) ))dnl
define(OPTION_PAIR,LIST_ITEM(BOLD($1 $2) ))dnl
define(OPTION_TRIPLET,LIST_ITEM(BOLD($1 --$2 PARAM($3)) ))dnl
define(OPTIONS_END,LIST_END)

define(LONGCODE_BEGIN,changequote([,])[changequote([,])```changequote]changequote)
define(LONGCODE_END,changequote([,])[changequote([,])```changequote]changequote)

define(FOOTER,CCTools CCTOOLS_VERSION released on CCTOOLS_RELEASE_DATE)dnl
