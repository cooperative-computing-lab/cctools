%{
#include "parser.h"
#include "parser.tab.h"
#include "ftsh_error.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#define YY_NO_UNPUT

static int grab( int type, int firstint );
static int grab_int( int type );

int current_line=1;

%}

DQUOTE		["]
NOTDQUOTE	[^"]
SQUOTE		[']
NOTSQUOTE	[^']
BQUOTE		[`]
ESCAPED         \\.
NOTSPECIAL      [^;,><&|\\'"` \t\n=()]
VARCHAR		[a-zA-Z0-9_]

STRING          ({NOTSPECIAL}|{ESCAPED})+
DQSTRING        {DQUOTE}({NOTDQUOTE}|{ESCAPED})*{DQUOTE}
SQSTRING        {SQUOTE}({NOTSQUOTE}|{ESCAPED})*{SQUOTE}

%%
#[^\n]*\n			current_line++;
[\t ]+				;
\\\n				current_line++;
\r?\n				{ grab(TOKEN_DELIMITER,0); current_line++; return TOKEN_DELIMITER; }
;				return grab(TOKEN_DELIMITER,0);

\>				return grab(TOKEN_RIGHT_ARROW,1);
[0-2]\>				return grab_int(TOKEN_RIGHT_ARROW);
\<				return grab(TOKEN_LEFT_ARROW,0);
[0-2]\<				return grab_int(TOKEN_LEFT_ARROW);
\>\>				return grab(TOKEN_DOUBLE_RIGHT_ARROW,1);
[0-2]\>\>			return grab_int(TOKEN_DOUBLE_RIGHT_ARROW);
[0-2]\>\&			return grab_int(TOKEN_NUMBERED_SQUIGGLY_RIGHT_ARROW);
[0-2]\>\>\&			return grab_int(TOKEN_NUMBERED_DOUBLE_SQUIGGLY_RIGHT_ARROW);
\>\&				return grab(TOKEN_SQUIGGLY_RIGHT_ARROW,0);
\&\>				return grab(TOKEN_SQUIGGLY_RIGHT_ARROW,0);
\>\>\&				return grab(TOKEN_DOUBLE_SQUIGGLY_RIGHT_ARROW,0);
\&\>\>				return grab(TOKEN_DOUBLE_SQUIGGLY_RIGHT_ARROW,0);
\-\>				return grab(TOKEN_LONG_RIGHT_ARROW,1);
[0-2]\-\>			return grab_int(TOKEN_LONG_RIGHT_ARROW);
\-\<				return grab(TOKEN_LONG_LEFT_ARROW,0);
[0-2]\-\<			return grab_int(TOKEN_LONG_LEFT_ARROW);
\-\>\>				return grab(TOKEN_LONG_DOUBLE_RIGHT_ARROW,1);
[0-2]\-\>\>			return grab_int(TOKEN_LONG_DOUBLE_RIGHT_ARROW);
\-\>\&				return grab_int(TOKEN_LONG_SQUIGGLY_RIGHT_ARROW);
\-\>\>\&			return grab_int(TOKEN_LONG_DOUBLE_SQUIGGLY_RIGHT_ARROW);
\<\<				ftsh_fatal(current_line," I don't support << 'here-file' (yet)\n");
[0-2]\<\<			ftsh_fatal(current_line," I don't support [n]<< 'here-file' (yet)\n");
\-\<\<				ftsh_fatal(current_line," I don't support -<< 'here-file' (yet)\n");
[0-2]\-\<\<			ftsh_fatal(current_line," I don't support [n]-<< 'here-file' (yet)\n");
\|				ftsh_fatal(current_line," I don't support | pipes (yet)\n");
\|\&				ftsh_fatal(current_line," I don't support |& pipes (yet)\n");
function			return grab(TOKEN_FUNCTION,0);
if				return grab(TOKEN_IF,0);
in				return grab(TOKEN_IN,0);
else				return grab(TOKEN_ELSE,0);
end				return grab(TOKEN_END,0);
try				return grab(TOKEN_TRY,0);
every				return grab(TOKEN_EVERY,0);
catch				return grab(TOKEN_CATCH,0);
for				return grab(TOKEN_FOR,0);
forany				return grab(TOKEN_FORANY,0);
forall				return grab(TOKEN_FORALL,0);
or				return grab(TOKEN_OR,0);
time				return grab(TOKEN_TIMES,0);
times				return grab(TOKEN_TIMES,0);
second				return grab(TOKEN_SECONDS,0);
seconds				return grab(TOKEN_SECONDS,0);
minute				return grab(TOKEN_MINUTES,0);
minutes				return grab(TOKEN_MINUTES,0);
hour				return grab(TOKEN_HOURS,0);
hours				return grab(TOKEN_HOURS,0);
day				return grab(TOKEN_DAYS,0);
days				return grab(TOKEN_DAYS,0);
shift				return grab(TOKEN_SHIFT,0);
while				return grab(TOKEN_WHILE,0);
return				return grab(TOKEN_RETURN,0);
\.add\.				return grab(TOKEN_ADD,0);
\.sub\.				return grab(TOKEN_SUB,0);
\.mul\.				return grab(TOKEN_MUL,0);
\.div\.				return grab(TOKEN_DIV,0);
\.mod\.				return grab(TOKEN_MOD,0);
\.pow\.				return grab(TOKEN_POW,0);
\.eq\.				return grab(TOKEN_EQ,0);
\.ne\.				return grab(TOKEN_NE,0);
\.eql\.				return grab(TOKEN_EQL,0);
\.neql\.			return grab(TOKEN_NEQL,0);
\.lt\.				return grab(TOKEN_LT,0);
\.gt\.				return grab(TOKEN_GT,0);
\.le\.				return grab(TOKEN_LE,0);
\.ge\.				return grab(TOKEN_GE,0);
\.and\.				return grab(TOKEN_AND,0);
\.or\.				return grab(TOKEN_OR,0);
\.not\.				return grab(TOKEN_NOT,0);
\.to\.				return grab(TOKEN_TO,0);
\.step\.			return grab(TOKEN_STEP,0);
\(				return grab(TOKEN_LPAREN,0);
\)				return grab(TOKEN_RPAREN,0);
\,				return grab(TOKEN_COMMA,0);
\.exists\.			return grab(TOKEN_EXISTS,0);
\.isr\.				return grab(TOKEN_ISR,0);
\.isw\.				return grab(TOKEN_ISW,0);
\.isx\.				return grab(TOKEN_ISX,0);
\.isblock\.			return grab(TOKEN_ISBLOCK,0);
\.ischar\.			return grab(TOKEN_ISCHAR,0);
\.isdir\.			return grab(TOKEN_ISDIR,0);
\.isfile\.			return grab(TOKEN_ISFILE,0);
\.islink\.			return grab(TOKEN_ISLINK,0);
\.ispipe\.			return grab(TOKEN_ISPIPE,0);
\.issock\.			return grab(TOKEN_ISSOCK,0);
{BQUOTE}			ftsh_fatal(current_line,"I don't support backticks (``).  Try variable redirection instead.");
{VARCHAR}+=			{ yytext[strlen(yytext)-1]=0; return grab(TOKEN_ASSIGN,0); }
{STRING}                        return grab(TOKEN_ATOM,0);
{SQSTRING}                      return grab(TOKEN_ATOM,0);
{DQSTRING}                      return grab(TOKEN_ATOM,0);
.				ftsh_fatal(current_line,"scan error near %s",yytext);
%%

static int grab_int( int type )
{
	yylval.token.line = current_line;
	yylval.token.type = type;
	yylval.token.firstint = yytext[0]-'0';
	return type;
}

static int grab( int type, int firstint )
{
	yylval.token.line = current_line;
	yylval.token.type = type;
	yylval.token.firstint = firstint;
	return type;
}

int yywrap()
{
	return 1;
}

