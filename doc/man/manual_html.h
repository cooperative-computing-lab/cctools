define(HEADER,<html><head><title>$1(1)</title></head><body><h1>$1(1)</h1>)dnl
define(SECTION,<h2>$1</h2>)dnl
define(SUBSECTION,<h3>$1</h3>)dnl
define(SUBSUBSECTION,<h4>$1</h4>)dnl
define(PARA,<p>)dnl
define(LINK,<a href=$2>$1</a>)dnl
define(MANUAL,LINK($1,$2))dnl
define(MANPAGE,LINK($1($2),$1.html))dnl
define(BOLD,<b>$1</b>)dnl
define(ITALIC,<i>$1</i>)dnl
define(CODE,<tt>$1</tt>)dnl

define(LIST_BEGIN,<ul>)
define(LIST_ITEM,<li>$1</li>)
define(LIST_END,</ul>)

define(SPACE,&nbsp;)
define(HALFTAB,SPACE()SPACE()SPACE()SPACE())
define(TAB,HALFTAB()HALFTAB())
define(PARAM,<i>&lt;$1&gt;</i>)

define(OPTIONS_BEGIN,<table>)dnl
define(OPTION_FLAG,<tr><td> CODE(-$1)`,'CODE(--$2) <td>)dnl
define(OPTION_FLAG_SHORT,<tr><td> CODE(-$1) <td>)dnl
define(OPTION_FLAG_LONG,<tr><td> CODE(--$1) <td>)dnl
define(OPTION_ARG,<tr><td> CODE(--$1)`,'CODE(--$2=$3)<td>)dnl
define(OPTION_ARG_SHORT,<tr><td> CODE(-$1 $2)<td>)dnl
define(OPTION_ARG_LONG,<tr><td> CODE(--$1=$2)<td>)dnl

define(OPTIONS_END,</table>)dnl

define(LONGCODE_BEGIN,<pre>)
define(LONGCODE_END,</pre>)

define(FOOTER,<p><hr>CCTools CCTOOLS_VERSION released on CCTOOLS_RELEASE_DATE</body></html>)dnl

define(TABLE_START,<table>)dnl
define(ROW,<tr valign=top>)dnl
define(COL,<td>)dnl
define(TABLE_END,</table>)dnl
define(CALLOUT,`<center><table width=75% border=2><tr bgcolor=#ffffaa><td>$1</table></center>')dnl
