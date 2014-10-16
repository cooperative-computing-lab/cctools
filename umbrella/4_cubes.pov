// This file is licensed under the terms of the CC-LGPL
// Minor changes made by Douglas Thain for CSE 40771

#declare Fast = no;

camera
{ location <0, 15, -30>
  look_at <0,0,5>
  angle 50
}

light_source
{ <20, 30, -10>, 1
  fade_distance 30
  fade_power 2
  #if(!Fast)
    area_light x*4, y*4, 16, 16 jitter adaptive 1 circular orient
  #end
}

light_source
{ <-20, 20, -15>, .5
  fade_distance 30
  fade_power 2
  #if(!Fast)
    area_light x*4, y*4, 16, 16 jitter adaptive 1 circular orient
  #end
}

plane
{ y, 0
  #if(Fast)
    pigment { rgb <1, .9, .7> }
  #else
    texture
    { average texture_map
      { #declare S = seed(0);
        #local ReflColor = .8*<1, .9, .7> + .2*<1,1,1>;
        #declare Ind = 0;
        #while(Ind < 20)
          [1 pigment { rgb <1, .9, .7> }
             normal { bumps .1 translate <rand(S),rand(S),rand(S)>*100 scale .001 }
             finish { reflection { ReflColor*.1, ReflColor*.5 } }
          ]
          #declare Ind = Ind+1;
        #end
      }
    }
  #end
}

#include "WRC_RubiksCube.inc"
object
{
WRC_RubiksRevenge("F'f' Rrd2R'r' U'u'Rr d2 U ld'l' U' ldl' R'r'Uu Ff")
translate < -6.6, 0, -6.6 >
rotate y*(45+clock*3600)
}

object
{
WRC_RubiksRevenge("F'f' Rrd2R'r' U'u'Rr d2 U ld'l' U' ldl' R'r'Uu Ff")
translate < -6.6, 0, -6.6 >
rotate y*(135+clock*3600)
translate < 6.6, 0, -6.6 >
}


object
{
WRC_RubiksRevenge("F'f' Rrd2R'r' U'u'Rr d2 U ld'l' U' ldl' R'r'Uu Ff")
translate < -6.6, 0, -6.6 >
rotate y*(215+clock*3600)
translate < 0, 0, -13.2>
}

object
{
WRC_RubiksRevenge("F'f' Rrd2R'r' U'u'Rr d2 U ld'l' U' ldl' R'r'Uu Ff")
translate < -6.6, 0, -6.6 >
rotate y*(305+clock*3600)
translate < -6.6, 0, -6.6>
}
