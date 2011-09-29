define(HEADER,<html><head><title>$1(1)</title></head><body><h1>$1(1)</h1>)dnl
define(SECTION,<h2>$1</h2>)dnl
define(SUBSECTION,<h3>$1</h3>)dnl
define(SUBSUBSECTION,<h4>$1</h4>)dnl
define(PARA,<p>)dnl
define(LINK,<a href=$2>$1</a>)dnl
define(MANPAGE,LINK($1($2),$1.html))dnl
define(BOLD,<b>$1</b>)dnl
define(CODE,<tt>$1</tt>)dnl

define(LIST_BEGIN,<dir>)
define(LIST_ITEM,<li>)
define(LIST_END,</dir>)

define(TAB,&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;)
define(PARAM,<i>&lt;$1&gt;</i>)

define(OPTIONS_BEGIN,<dir>)
define(OPTION_ITEM,<li> BOLD(CODE($1))TAB())
define(OPTION_PAIR,<li> BOLD(CODE($1)) PARAM($2)<br/>TAB()TAB())
define(OPTIONS_END,</dir>)

define(LONGCODE_BEGIN,<pre>)
define(LONGCODE_END,</pre>)

define(FOOTER,</body></html>)dnl

define(TABLE_START,<table>)dnl
define(ROW,<tr valign=top>)dnl
define(COL,<td>)dnl
define(TABLE_END,</table>)dnl
define(CALLOUT,`<center><table width=75% border=2><tr bgcolor=#ffffaa><td>$1</table></center>')dnl
