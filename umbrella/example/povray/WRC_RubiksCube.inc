//This file is licensed under the terms of the CC-LGPL

/*============================================================================
  Rubik's Cube (3x3x3) and Rubik's Revenge (4x4x4) include file
  -------------------------------------------------------------
  Usage:

  WRC_RubiksCube("...")
  WRC_RubiksRevenge("...")

  The parameter is a string containing rotations (an empty string is ok).

  Uses actual measures of the real Rubik's cubes.
  1 pov unit = 1 cm
  Rubik's Cube width = 5.7 cm + 0.02 cm for stickers
  Rubik's Revenge width = 6.6 cm + 0.02 cm for stickers
  The lower-left-front corner of the cubes is located at <0, 0, 0>.

  Rotation syntax:

  - A rotation character (U, F, L, R, B, D) rotates the correspondent
    outermost slice clockwise (U=upper, F=front, L=left, R=right,
    B=back, D=down).

  - For Rubik's Revenge, if written in lowercase, rotates the inner slice.

  - An aphostrophe (') immediately following the character rotates the
    slice counter-clockwise.

  - The character '2' immediately following the character rotates the
    slice twice.

  - Spaces between rotations are optional.

  Examples:

  WRC_RubiksCube("F2 D L2 D R U R L' B' D U' B' R' F2 R' U2")
  WRC_RubiksRevenge("U L' F f L2 F2 D")

  The last rotation can be made incomplete by declaring the following
  float identifier before calling the macros:

  WRC_LastMoveRotationFactor

  The value should be a factor between 0 and 1, where 0 means that the
  last rotation is not made at all, and 1 means that it's done as normal.
  Values between 0 and 1 will rotate it by that relative amount. (Note
  that in the parameter string there must not be any whitespace after the
  last rotation for this to work.)
==============================================================================
*/


#ifndef(WRC_LastMoveRotationFactor)
  #declare WRC_LastMoveRotationFactor = 1;
#end


// Creates a color sticker
// (Default position is for the WRC_CenterPiece() below).
#macro WRC_ColorSticker(Color)
  union
  { #local Center1 = <1, 0, 1>*WRC_PieceEdgeRadius*2;
    #local Center2 = Center1 + x*(WRC_PieceWidth-WRC_PieceEdgeRadius*4);
    #local Center3 = Center2 + z*(WRC_PieceWidth-WRC_PieceEdgeRadius*4);
    #local Center4 = Center1 + z*(WRC_PieceWidth-WRC_PieceEdgeRadius*4);

    cylinder { Center1, Center1+y*.01, WRC_PieceEdgeRadius/2 }
    cylinder { Center2, Center2+y*.01, WRC_PieceEdgeRadius/2 }
    cylinder { Center3, Center3+y*.01, WRC_PieceEdgeRadius/2 }
    cylinder { Center4, Center4+y*.01, WRC_PieceEdgeRadius/2 }
    box { Center1-z*WRC_PieceEdgeRadius/2,
      Center3+<0, .01, WRC_PieceEdgeRadius/2> }
    box { Center1-x*WRC_PieceEdgeRadius/2,
      Center3+<WRC_PieceEdgeRadius/2, .01, 0> }

    pigment { rgb Color }
    finish { specular .5 }
  }
#end

// Creates a centerpiece, oriented up.
// Left-front corner at origin, upper surface at y=0.
#macro WRC_CenterPiece(Color)
  union
  { WRC_ColorSticker(Color)

    #local Center1 = <1, -1, 1>*WRC_PieceEdgeRadius;
    #local Center1b = Center1-y*(WRC_PieceWidth-WRC_PieceEdgeRadius*2);
    #local Center2 = Center1 + x*(WRC_PieceWidth-WRC_PieceEdgeRadius*2);
    #local Center2b = Center2-y*(WRC_PieceWidth-WRC_PieceEdgeRadius*2);
    #local Center3 = Center2 + z*(WRC_PieceWidth-WRC_PieceEdgeRadius*2);
    #local Center3b = Center3-y*(WRC_PieceWidth-WRC_PieceEdgeRadius*2);
    #local Center4 = Center1 + z*(WRC_PieceWidth-WRC_PieceEdgeRadius*2);
    #local Center4b = Center4-y*(WRC_PieceWidth-WRC_PieceEdgeRadius*2);

    sphere { Center1, WRC_PieceEdgeRadius }
    sphere { Center2, WRC_PieceEdgeRadius }
    sphere { Center3, WRC_PieceEdgeRadius }
    sphere { Center4, WRC_PieceEdgeRadius }
    cylinder { Center1, Center2, WRC_PieceEdgeRadius }
    cylinder { Center2, Center3, WRC_PieceEdgeRadius }
    cylinder { Center3, Center4, WRC_PieceEdgeRadius }
    cylinder { Center4, Center1, WRC_PieceEdgeRadius }
    cylinder { Center1, Center1b, WRC_PieceEdgeRadius }
    cylinder { Center2, Center2b, WRC_PieceEdgeRadius }
    cylinder { Center3, Center3b, WRC_PieceEdgeRadius }
    cylinder { Center4, Center4b, WRC_PieceEdgeRadius }
    box { Center1*<1,0,1>, Center3 }
    box { Center1*<1,1,0>, Center2b }
    box { Center1*<0,1,1>, Center4b }
    box { Center2+x*WRC_PieceEdgeRadius, Center3b }
    box { Center4+z*WRC_PieceEdgeRadius, Center3b }
  }
#end

// Creates an edge piece, oriented up-left.
#macro WRC_EdgePiece(UpColor, LeftColor)
  union
  { WRC_CenterPiece(UpColor)
    object { WRC_ColorSticker(LeftColor) scale <1, -1, 1> rotate z*-90 }

    #local Center1 = <1, -1, 1>*WRC_PieceEdgeRadius -
      y*(WRC_PieceWidth-WRC_PieceEdgeRadius*2);
    #local Center2 = Center1 + x*(WRC_PieceWidth-WRC_PieceEdgeRadius*2);
    #local Center3 = Center2 + z*(WRC_PieceWidth-WRC_PieceEdgeRadius*2);
    #local Center4 = Center1 + z*(WRC_PieceWidth-WRC_PieceEdgeRadius*2);

    sphere { Center1, WRC_PieceEdgeRadius }
    sphere { Center4, WRC_PieceEdgeRadius }
    cylinder { Center1, Center4, WRC_PieceEdgeRadius }
    cylinder { Center1, Center2, WRC_PieceEdgeRadius }
    cylinder { Center4, Center3, WRC_PieceEdgeRadius }
    box { Center1-y*WRC_PieceEdgeRadius, Center3 }
  }
#end

// Creates a corner piece, oriented up-left-front.
#macro WRC_CornerPiece(UpColor, LeftColor, FrontColor)
  union
  { WRC_EdgePiece(UpColor, LeftColor)
    object { WRC_ColorSticker(FrontColor) scale <1, -1, 1> rotate x*90 }

    #local Center1 = <1, -1, 1>*WRC_PieceEdgeRadius -
      y*(WRC_PieceWidth-WRC_PieceEdgeRadius*2);
    #local Center2 = Center1 + x*(WRC_PieceWidth-WRC_PieceEdgeRadius*2);
    #local Center3 = Center2 + z*(WRC_PieceWidth-WRC_PieceEdgeRadius*2);

    sphere { Center2, WRC_PieceEdgeRadius }
    cylinder { Center2, Center3, WRC_PieceEdgeRadius }
  }
#end

#macro WRC_MovePieceRC(Sx, Sy, Sz, Dx, Dy, Dz, Rotation)
  #declare WRC_RCData[Dx][Dy][Dz] =
    object
    { WRC_RCData[Sx][Sy][Sz]
      translate -WRC_PieceWidth*3/2
      rotate Rotation
      translate WRC_PieceWidth*3/2
    };
#end

#macro WRC_MovePieceRR(Sx, Sy, Sz, Dx, Dy, Dz, Rotation)
  #declare WRC_RRData[Dx][Dy][Dz] =
    object
    { WRC_RRData[Sx][Sy][Sz]
      translate -WRC_PieceWidth*4/2
      rotate Rotation
      translate WRC_PieceWidth*4/2
    };
#end

#macro WRC_RotateXZLayerRC(LayerInd, RF)
  #local Ind = 0;
  #while(Ind < 2)
    #local tmp = WRC_RCData[Ind][LayerInd][0];
    WRC_MovePieceRC(2, LayerInd, Ind, Ind, LayerInd, 0, y*90*RF)
    WRC_MovePieceRC(2-Ind, LayerInd, 2, 2, LayerInd, Ind, y*90*RF)
    WRC_MovePieceRC(0, LayerInd, 2-Ind, 2-Ind, LayerInd, 2, y*90*RF)
    #declare WRC_RCData[0][LayerInd][2-Ind] = tmp;
    WRC_MovePieceRC(0, LayerInd, 2-Ind, 0, LayerInd, 2-Ind, y*90*RF)
    #local Ind = Ind+1;
  #end
  #if(RF != 1)
    #ifdef(WRC_RCData[1][LayerInd][1])
      WRC_MovePieceRC(1, LayerInd, 1, 1, LayerInd, 1, y*90*RF)
    #end
  #end
#end

#macro WRC_RotateXZLayerRR(LayerInd, RF)
  #local Ind = 0;
  #while(Ind < 3)
    #local tmp = WRC_RRData[Ind][LayerInd][0];
    WRC_MovePieceRR(3, LayerInd, Ind, Ind, LayerInd, 0, y*90*RF)
    WRC_MovePieceRR(3-Ind, LayerInd, 3, 3, LayerInd, Ind, y*90*RF)
    WRC_MovePieceRR(0, LayerInd, 3-Ind, 3-Ind, LayerInd, 3, y*90*RF)
    #declare WRC_RRData[0][LayerInd][3-Ind] = tmp;
    WRC_MovePieceRR(0, LayerInd, 3-Ind, 0, LayerInd, 3-Ind, y*90*RF)
    #local Ind = Ind+1;
  #end
  #ifdef(WRC_RRData[1][LayerInd][1])
    #local tmp = WRC_RRData[1][LayerInd][1];
    WRC_MovePieceRR(2, LayerInd, 1, 1, LayerInd, 1, y*90*RF)
    WRC_MovePieceRR(2, LayerInd, 2, 2, LayerInd, 1, y*90*RF)
    WRC_MovePieceRR(1, LayerInd, 2, 2, LayerInd, 2, y*90*RF)
    #declare WRC_RRData[1][LayerInd][2] = tmp;
    WRC_MovePieceRR(1, LayerInd, 2, 1, LayerInd, 2, y*90*RF)
  #end
#end

#macro WRC_RotateXYLayerRC(LayerInd, RF)
  #local Ind = 0;
  #while(Ind < 2)
    #local tmp = WRC_RCData[Ind][0][LayerInd];
    WRC_MovePieceRC(2, Ind, LayerInd, Ind, 0, LayerInd, -z*90*RF)
    WRC_MovePieceRC(2-Ind, 2, LayerInd, 2, Ind, LayerInd, -z*90*RF)
    WRC_MovePieceRC(0, 2-Ind, LayerInd, 2-Ind, 2, LayerInd, -z*90*RF)
    #declare WRC_RCData[0][2-Ind][LayerInd] = tmp;
    WRC_MovePieceRC(0, 2-Ind, LayerInd, 0, 2-Ind, LayerInd, -z*90*RF)
    #local Ind = Ind+1;
  #end
  #if(RF != 1)
    #ifdef(WRC_RCData[1][1][LayerInd])
      WRC_MovePieceRC(1, 1, LayerInd, 1, 1, LayerInd, -z*90*RF)
    #end
  #end
#end

#macro WRC_RotateXYLayerRR(LayerInd, RF)
  #local Ind = 0;
  #while(Ind < 3)
    #local tmp = WRC_RRData[Ind][0][LayerInd];
    WRC_MovePieceRR(3, Ind, LayerInd, Ind, 0, LayerInd, -z*90*RF)
    WRC_MovePieceRR(3-Ind, 3, LayerInd, 3, Ind, LayerInd, -z*90*RF)
    WRC_MovePieceRR(0, 3-Ind, LayerInd, 3-Ind, 3, LayerInd, -z*90*RF)
    #declare WRC_RRData[0][3-Ind][LayerInd] = tmp;
    WRC_MovePieceRR(0, 3-Ind, LayerInd, 0, 3-Ind, LayerInd, -z*90*RF)
    #local Ind = Ind+1;
  #end
  #ifdef(WRC_RRData[1][1][LayerInd])
    #local tmp = WRC_RRData[1][1][LayerInd];
    WRC_MovePieceRR(2, 1, LayerInd, 1, 1, LayerInd, -z*90*RF)
    WRC_MovePieceRR(2, 2, LayerInd, 2, 1, LayerInd, -z*90*RF)
    WRC_MovePieceRR(1, 2, LayerInd, 2, 2, LayerInd, -z*90*RF)
    #declare WRC_RRData[1][2][LayerInd] = tmp;
    WRC_MovePieceRR(1, 2, LayerInd, 1, 2, LayerInd, -z*90*RF)
  #end
#end

#macro WRC_RotateYZLayerRC(LayerInd, RF)
  #local Ind = 0;
  #while(Ind < 2)
    #local tmp = WRC_RCData[LayerInd][Ind][0];
    WRC_MovePieceRC(LayerInd, 2, Ind, LayerInd, Ind, 0, -x*90*RF)
    WRC_MovePieceRC(LayerInd, 2-Ind, 2, LayerInd, 2, Ind, -x*90*RF)
    WRC_MovePieceRC(LayerInd, 0, 2-Ind, LayerInd, 2-Ind, 2, -x*90*RF)
    #declare WRC_RCData[LayerInd][0][2-Ind] = tmp;
    WRC_MovePieceRC(LayerInd, 0, 2-Ind, LayerInd, 0, 2-Ind, -x*90*RF)
    #local Ind = Ind+1;
  #end
  #if(RF != 1)
    #ifdef(WRC_RCData[LayerInd][1][1])
      WRC_MovePieceRC(LayerInd, 1, 1, LayerInd, 1, 1, -x*90*RF)
    #end
  #end
#end

#macro WRC_RotateYZLayerRR(LayerInd, RF)
  #local Ind = 0;
  #while(Ind < 3)
    #local tmp = WRC_RRData[LayerInd][Ind][0];
    WRC_MovePieceRR(LayerInd, 3, Ind, LayerInd, Ind, 0, -x*90*RF)
    WRC_MovePieceRR(LayerInd, 3-Ind, 3, LayerInd, 3, Ind, -x*90*RF)
    WRC_MovePieceRR(LayerInd, 0, 3-Ind, LayerInd, 3-Ind, 3, -x*90*RF)
    #declare WRC_RRData[LayerInd][0][3-Ind] = tmp;
    WRC_MovePieceRR(LayerInd, 0, 3-Ind, LayerInd, 0, 3-Ind, -x*90*RF)
    #local Ind = Ind+1;
  #end
  #ifdef(WRC_RRData[LayerInd][1][1])
    #local tmp = WRC_RRData[LayerInd][1][1];
    WRC_MovePieceRR(LayerInd, 2, 1, LayerInd, 1, 1, -x*90*RF)
    WRC_MovePieceRR(LayerInd, 2, 2, LayerInd, 2, 1, -x*90*RF)
    WRC_MovePieceRR(LayerInd, 1, 2, LayerInd, 2, 2, -x*90*RF)
    #declare WRC_RRData[LayerInd][1][2] = tmp;
    WRC_MovePieceRR(LayerInd, 1, 2, LayerInd, 1, 2, -x*90*RF)
  #end
#end

#macro WRC_RubiksCube(Moves)
  #local WRC_PieceWidth = 5.7/3;
  #local WRC_PieceEdgeRadius = .1;

  #local WRC_RCData = array[3][3][3];

  #local UpColor = <1, 1, 1>;
  #local FrontColor = <1, 0, 0>;
  #local LeftColor = <0, 1, 0>;
  #local RightColor = <0, 0, 1>;
  #local BackColor = <1, .5, 0>;
  #local DownColor = <1, 1, 0>;

  #local WRC_RCData[0][0][0] =
    object { WRC_CornerPiece(FrontColor, LeftColor, DownColor) rotate x*-90 };
  #local WRC_RCData[1][0][0] =
    object { WRC_EdgePiece(FrontColor, DownColor)
             rotate y*-90 rotate x*-90 translate x*2*WRC_PieceWidth }
  #local WRC_RCData[2][0][0] =
    object { WRC_CornerPiece(FrontColor, DownColor, RightColor)
             rotate y*-90 rotate x*-90 translate x*3*WRC_PieceWidth }

  #local WRC_RCData[0][0][1] =
    object { WRC_EdgePiece(LeftColor, DownColor)
             rotate z*90 translate z*WRC_PieceWidth }
  #local WRC_RCData[1][0][1] =
    object { WRC_CenterPiece(DownColor)
             rotate z*180 translate <2, 0, 1>*WRC_PieceWidth }
  #local WRC_RCData[2][0][1] =
    object { WRC_EdgePiece(DownColor, RightColor)
             rotate z*180 translate <3, 0, 1>*WRC_PieceWidth }

  #local WRC_RCData[0][0][2] =
    object { WRC_CornerPiece(DownColor, LeftColor, BackColor)
             rotate x*180 translate z*3*WRC_PieceWidth }
  #local WRC_RCData[1][0][2] =
    object { WRC_EdgePiece(BackColor, DownColor)
             rotate y*90 rotate x*90 translate <1, 0, 3>*WRC_PieceWidth }
  #local WRC_RCData[2][0][2] =
    object { WRC_CornerPiece(DownColor, BackColor, RightColor)
             rotate x*180 rotate y*90 translate <3, 0, 3>*WRC_PieceWidth }

  #local WRC_RCData[0][1][0] =
    object { WRC_EdgePiece(FrontColor, LeftColor)
             rotate x*-90 translate y*WRC_PieceWidth }
  #local WRC_RCData[1][1][0] =
    object { WRC_CenterPiece(FrontColor)
             rotate x*-90 translate <1, 1, 0>*WRC_PieceWidth }
  #local WRC_RCData[2][1][0] =
    object { WRC_EdgePiece(RightColor, FrontColor)
             rotate <-90, -90, 0> translate <3, 1, 0>*WRC_PieceWidth }

  #local WRC_RCData[0][1][1] =
    object { WRC_CenterPiece(LeftColor)
             rotate z*90 translate <0, 1, 1>*WRC_PieceWidth }
  #local WRC_RCData[2][1][1] =
    object { WRC_CenterPiece(RightColor)
             rotate z*-90 translate <3, 2, 1>*WRC_PieceWidth }

  #local WRC_RCData[0][1][2] =
    object { WRC_EdgePiece(BackColor, LeftColor)
             rotate x*90 translate <0, 2, 3>*WRC_PieceWidth }
  #local WRC_RCData[1][1][2] =
    object { WRC_CenterPiece(BackColor)
             rotate x*90 translate <1, 2, 3>*WRC_PieceWidth }
  #local WRC_RCData[2][1][2] =
    object { WRC_EdgePiece(RightColor, BackColor)
             rotate <90, 90, 0> translate <3, 2, 3>*WRC_PieceWidth }

  #local WRC_RCData[0][2][0] =
    object { WRC_CornerPiece(UpColor, LeftColor, FrontColor)
             translate y*3*WRC_PieceWidth }
  #local WRC_RCData[1][2][0] =
    object { WRC_EdgePiece(UpColor, FrontColor)
             rotate y*-90 translate <2, 3, 0>*WRC_PieceWidth }
  #local WRC_RCData[2][2][0] =
    object { WRC_CornerPiece(UpColor, FrontColor, RightColor)
             rotate y*-90 translate <3, 3, 0>*WRC_PieceWidth }

  #local WRC_RCData[0][2][1] =
    object { WRC_EdgePiece(UpColor, LeftColor)
             translate <0, 3, 1>*WRC_PieceWidth }
  #local WRC_RCData[1][2][1] =
    object { WRC_CenterPiece(UpColor) translate <1, 3, 1>*WRC_PieceWidth }
  #local WRC_RCData[2][2][1] =
    object { WRC_EdgePiece(UpColor, RightColor)
             rotate y*180 translate <3, 3, 2>*WRC_PieceWidth }

  #local WRC_RCData[0][2][2] =
    object { WRC_CornerPiece(UpColor, BackColor, LeftColor)
             rotate y*90 translate <0, 3, 3>*WRC_PieceWidth }
  #local WRC_RCData[1][2][2] =
    object { WRC_EdgePiece(UpColor, BackColor)
             rotate y*90 translate <1, 3, 3>*WRC_PieceWidth }
  #local WRC_RCData[2][2][2] =
    object { WRC_CornerPiece(UpColor, RightColor, BackColor)
             rotate y*180 translate 3*WRC_PieceWidth }


  #local RF = 1;
  #local RRF = 1;
  #local MSize = strlen(Moves);
  #local Ind = 1;
  #while(Ind <= MSize)
    #local Rot = asc(substr(Moves, Ind, 1));
    #local CCW = false;
    #if(Ind < MSize)
      #if(asc(substr(Moves, Ind+1, 1)) = 39)
        #local CCW = true;
        #local Ind = Ind+1;
      #end
    #end
    #local Double = false;
    #if(Ind < MSize)
      #if(asc(substr(Moves, Ind+1, 1)) = 50)
        #local Double = true;
        #local Ind = Ind+1;
      #end
    #end

    #if(Ind = MSize)
      #local RF = WRC_LastMoveRotationFactor;
      #local RRF = 1+(1-RF);
    #end

    #switch(Rot)
      #case(85) // 'U'
        WRC_RotateXZLayerRC(2, (CCW ? RRF : RF))
        #if(Double) WRC_RotateXZLayerRC(2, 1) #end
        #if(CCW) WRC_RotateXZLayerRC(2, 1) WRC_RotateXZLayerRC(2, 1) #end
        #break
      #case(70) // 'F'
        WRC_RotateXYLayerRC(0, (CCW ? RRF : RF))
        #if(Double) WRC_RotateXYLayerRC(0, 1) #end
        #if(CCW) WRC_RotateXYLayerRC(0, 1) WRC_RotateXYLayerRC(0, 1) #end
        #break
      #case(76) // 'L'
        WRC_RotateYZLayerRC(0, (CCW ? RRF : RF))
        #if(Double) WRC_RotateYZLayerRC(0, 1) #end
        #if(CCW) WRC_RotateYZLayerRC(0, 1) WRC_RotateYZLayerRC(0, 1) #end
        #break
      #case(82) // 'R'
        WRC_RotateYZLayerRC(2, (CCW ? RF : RRF))
        #if(Double) WRC_RotateYZLayerRC(2, 1) #else
          #if(!CCW) WRC_RotateYZLayerRC(2, 1) WRC_RotateYZLayerRC(2, 1) #end
        #end
        #break
      #case(66) // 'B'
        WRC_RotateXYLayerRC(2, (CCW ? RF : RRF))
        #if(Double) WRC_RotateXYLayerRC(2, 1) #else
          #if(!CCW) WRC_RotateXYLayerRC(2, 1) WRC_RotateXYLayerRC(2, 1) #end
        #end
        #break
      #case(68) // 'D'
        WRC_RotateXZLayerRC(0, (CCW ? RF : RRF))
        #if(Double) WRC_RotateXZLayerRC(0, 1) #else
          #if(!CCW) WRC_RotateXZLayerRC(0, 1) WRC_RotateXZLayerRC(0, 1) #end
        #end
        #break
    #end

    #local Ind = Ind+1;
  #end

  union
  { #local XInd = 0;
    #while(XInd < 3)
      #local YInd = 0;
      #while(YInd < 3)
        #local ZInd = 0;
        #while(ZInd < 3)
          #ifdef(WRC_RCData[XInd][YInd][ZInd])
            object { WRC_RCData[XInd][YInd][ZInd] }
          #end
          #local ZInd = ZInd+1;
        #end
        #local YInd = YInd+1;
      #end
      #local XInd = XInd+1;
    #end

    pigment { rgb 0 }
    finish { specular .5 roughness .1 }
  }
#end

#macro WRC_RubiksRevenge(Moves)
  #local WRC_PieceWidth = 1.65;
  #local WRC_PieceEdgeRadius = .1;

  #local WRC_RRData = array[4][4][4];

  #local UpColor = <1, 1, 1>;
  #local FrontColor = <1, 0, 0>;
  #local LeftColor = <0, 1, 0>;
  #local RightColor = <0, 0, 1>;
  #local BackColor = <1, .5, 0>;
  #local DownColor = <1, 1, 0>;

  #local WRC_RRData[0][0][0] =
    object { WRC_CornerPiece(FrontColor, LeftColor, DownColor) rotate x*-90 };
  #local WRC_RRData[1][0][0] =
    object { WRC_EdgePiece(FrontColor, DownColor)
             rotate y*-90 rotate x*-90 translate x*2*WRC_PieceWidth }
  #local WRC_RRData[2][0][0] =
    object { WRC_RRData[1][0][0] translate x*WRC_PieceWidth }
  #local WRC_RRData[3][0][0] =
    object { WRC_CornerPiece(FrontColor, DownColor, RightColor)
             rotate y*-90 rotate x*-90 translate x*4*WRC_PieceWidth }

  #local WRC_RRData[0][0][1] =
    object { WRC_EdgePiece(LeftColor, DownColor)
             rotate z*90 translate z*WRC_PieceWidth }
  #local WRC_RRData[1][0][1] =
    object { WRC_CenterPiece(DownColor)
             rotate z*180 translate <2, 0, 1>*WRC_PieceWidth }
  #local WRC_RRData[2][0][1] =
    object { WRC_RRData[1][0][1] translate x*WRC_PieceWidth }
  #local WRC_RRData[3][0][1] =
    object { WRC_EdgePiece(DownColor, RightColor)
             rotate z*180 translate <4, 0, 1>*WRC_PieceWidth }

  #local WRC_RRData[0][0][2] =
    object { WRC_RRData[0][0][1] translate z*WRC_PieceWidth }
  #local WRC_RRData[1][0][2] =
    object { WRC_RRData[1][0][1] translate z*WRC_PieceWidth }
  #local WRC_RRData[2][0][2] =
    object { WRC_RRData[2][0][1] translate z*WRC_PieceWidth }
  #local WRC_RRData[3][0][2] =
    object { WRC_RRData[3][0][1] translate z*WRC_PieceWidth }

  #local WRC_RRData[0][0][3] =
    object { WRC_CornerPiece(DownColor, LeftColor, BackColor)
             rotate x*180 translate z*4*WRC_PieceWidth }
  #local WRC_RRData[1][0][3] =
    object { WRC_EdgePiece(BackColor, DownColor)
             rotate y*90 rotate x*90 translate <1, 0, 4>*WRC_PieceWidth }
  #local WRC_RRData[2][0][3] =
    object { WRC_RRData[1][0][3] translate x*WRC_PieceWidth }
  #local WRC_RRData[3][0][3] =
    object { WRC_CornerPiece(DownColor, BackColor, RightColor)
             rotate x*180 rotate y*90 translate <4, 0, 4>*WRC_PieceWidth }

  #local WRC_RRData[0][1][0] =
    object { WRC_EdgePiece(FrontColor, LeftColor)
             rotate x*-90 translate y*WRC_PieceWidth }
  #local WRC_RRData[1][1][0] =
    object { WRC_CenterPiece(FrontColor)
             rotate x*-90 translate <1, 1, 0>*WRC_PieceWidth }
  #local WRC_RRData[2][1][0] =
    object { WRC_RRData[1][1][0] translate x*WRC_PieceWidth }
  #local WRC_RRData[3][1][0] =
    object { WRC_EdgePiece(RightColor, FrontColor)
             rotate <-90, -90, 0> translate <4, 1, 0>*WRC_PieceWidth }

  #local WRC_RRData[0][1][1] =
    object { WRC_CenterPiece(LeftColor)
             rotate z*90 translate <0, 1, 1>*WRC_PieceWidth }
  #local WRC_RRData[3][1][1] =
    object { WRC_CenterPiece(RightColor)
             rotate z*-90 translate <4, 2, 1>*WRC_PieceWidth }

  #local WRC_RRData[0][1][2] =
    object { WRC_RRData[0][1][1] translate z*WRC_PieceWidth }
  #local WRC_RRData[3][1][2] =
    object { WRC_RRData[3][1][1] translate z*WRC_PieceWidth }

  #local WRC_RRData[0][1][3] =
    object { WRC_EdgePiece(BackColor, LeftColor)
             rotate x*90 translate <0, 2, 4>*WRC_PieceWidth }
  #local WRC_RRData[1][1][3] =
    object { WRC_CenterPiece(BackColor)
             rotate x*90 translate <1, 2, 4>*WRC_PieceWidth }
  #local WRC_RRData[2][1][3] =
    object { WRC_RRData[1][1][3] translate x*WRC_PieceWidth }
  #local WRC_RRData[3][1][3] =
    object { WRC_EdgePiece(RightColor, BackColor)
             rotate <90, 90, 0> translate <4, 2, 4>*WRC_PieceWidth }

  #local WRC_RRData[0][2][0] =
    object { WRC_RRData[0][1][0] translate y*WRC_PieceWidth }
  #local WRC_RRData[1][2][0] =
    object { WRC_RRData[1][1][0] translate y*WRC_PieceWidth }
  #local WRC_RRData[2][2][0] =
    object { WRC_RRData[2][1][0] translate y*WRC_PieceWidth }
  #local WRC_RRData[3][2][0] =
    object { WRC_RRData[3][1][0] translate y*WRC_PieceWidth }

  #local WRC_RRData[0][2][1] =
    object { WRC_RRData[0][1][1] translate y*WRC_PieceWidth }
  #local WRC_RRData[3][2][1] =
    object { WRC_RRData[3][1][1] translate y*WRC_PieceWidth }
  #local WRC_RRData[0][2][2] =
    object { WRC_RRData[0][1][2] translate y*WRC_PieceWidth }
  #local WRC_RRData[3][2][2] =
    object { WRC_RRData[3][1][2] translate y*WRC_PieceWidth }

  #local WRC_RRData[0][2][3] =
    object { WRC_RRData[0][1][3] translate y*WRC_PieceWidth }
  #local WRC_RRData[1][2][3] =
    object { WRC_RRData[1][1][3] translate y*WRC_PieceWidth }
  #local WRC_RRData[2][2][3] =
    object { WRC_RRData[2][1][3] translate y*WRC_PieceWidth }
  #local WRC_RRData[3][2][3] =
    object { WRC_RRData[3][1][3] translate y*WRC_PieceWidth }

  #local WRC_RRData[0][3][0] =
    object { WRC_CornerPiece(UpColor, LeftColor, FrontColor)
             translate y*4*WRC_PieceWidth }
  #local WRC_RRData[1][3][0] =
    object { WRC_EdgePiece(UpColor, FrontColor)
             rotate y*-90 translate <2, 4, 0>*WRC_PieceWidth }
  #local WRC_RRData[2][3][0] =
    object { WRC_RRData[1][3][0] translate x*WRC_PieceWidth }
  #local WRC_RRData[3][3][0] =
    object { WRC_CornerPiece(UpColor, FrontColor, RightColor)
             rotate y*-90 translate <4, 4, 0>*WRC_PieceWidth }

  #local WRC_RRData[0][3][1] =
    object { WRC_EdgePiece(UpColor, LeftColor)
             translate <0, 4, 1>*WRC_PieceWidth }
  #local WRC_RRData[1][3][1] =
    object { WRC_CenterPiece(UpColor) translate <1, 4, 1>*WRC_PieceWidth }
  #local WRC_RRData[2][3][1] =
    object { WRC_RRData[1][3][1] translate x*WRC_PieceWidth }
  #local WRC_RRData[3][3][1] =
    object { WRC_EdgePiece(UpColor, RightColor)
             rotate y*180 translate <4, 4, 2>*WRC_PieceWidth }

  #local WRC_RRData[0][3][2] =
    object { WRC_RRData[0][3][1] translate z*WRC_PieceWidth }
  #local WRC_RRData[1][3][2] =
    object { WRC_RRData[1][3][1] translate z*WRC_PieceWidth }
  #local WRC_RRData[2][3][2] =
    object { WRC_RRData[2][3][1] translate z*WRC_PieceWidth }
  #local WRC_RRData[3][3][2] =
    object { WRC_RRData[3][3][1] translate z*WRC_PieceWidth }

  #local WRC_RRData[0][3][3] =
    object { WRC_CornerPiece(UpColor, BackColor, LeftColor)
             rotate y*90 translate <0, 4, 4>*WRC_PieceWidth }
  #local WRC_RRData[1][3][3] =
    object { WRC_EdgePiece(UpColor, BackColor)
             rotate y*90 translate <1, 4, 4>*WRC_PieceWidth }
  #local WRC_RRData[2][3][3] =
    object { WRC_RRData[1][3][3] translate x*WRC_PieceWidth }
  #local WRC_RRData[3][3][3] =
    object { WRC_CornerPiece(UpColor, RightColor, BackColor)
             rotate y*180 translate 4*WRC_PieceWidth }


  #local RF = 1;
  #local RRF = 1;
  #local MSize = strlen(Moves);
  #local Ind = 1;
  #while(Ind <= MSize)
    #local Rot = asc(substr(Moves, Ind, 1));
    #local CCW = false;
    #if(Ind < MSize)
      #if(asc(substr(Moves, Ind+1, 1)) = 39)
        #local CCW = true;
        #local Ind = Ind+1;
      #end
    #end
    #local Double = false;
    #if(Ind < MSize)
      #if(asc(substr(Moves, Ind+1, 1)) = 50)
        #local Double = true;
        #local Ind = Ind+1;
      #end
    #end

    #if(Ind = MSize)
      #local RF = WRC_LastMoveRotationFactor;
      #local RRF = 1+(1-RF);
    #end

    #switch(Rot)
      #case(85) // 'U'
        WRC_RotateXZLayerRR(3, (CCW ? RRF : RF))
        #if(Double) WRC_RotateXZLayerRR(3, 1) #end
        #if(CCW) WRC_RotateXZLayerRR(3, 1) WRC_RotateXZLayerRR(3, 1) #end
        #break
      #case(117) // 'u'
        WRC_RotateXZLayerRR(2, (CCW ? RRF : RF))
        #if(Double) WRC_RotateXZLayerRR(2, 1) #end
        #if(CCW) WRC_RotateXZLayerRR(2, 1) WRC_RotateXZLayerRR(2, 1) #end
        #break
      #case(70) // 'F'
        WRC_RotateXYLayerRR(0, (CCW ? RRF : RF))
        #if(Double) WRC_RotateXYLayerRR(0, 1) #end
        #if(CCW) WRC_RotateXYLayerRR(0, 1) WRC_RotateXYLayerRR(0, 1) #end
        #break
      #case(102) // 'f'
        WRC_RotateXYLayerRR(1, (CCW ? RRF : RF))
        #if(Double) WRC_RotateXYLayerRR(1, 1) #end
        #if(CCW) WRC_RotateXYLayerRR(1, 1) WRC_RotateXYLayerRR(1, 1) #end
        #break
      #case(76) // 'L'
        WRC_RotateYZLayerRR(0, (CCW ? RRF : RF))
        #if(Double) WRC_RotateYZLayerRR(0, 1) #end
        #if(CCW) WRC_RotateYZLayerRR(0, 1) WRC_RotateYZLayerRR(0, 1) #end
        #break
      #case(108) // 'l'
        WRC_RotateYZLayerRR(1, (CCW ? RRF : RF))
        #if(Double) WRC_RotateYZLayerRR(1, 1) #end
        #if(CCW) WRC_RotateYZLayerRR(1, 1) WRC_RotateYZLayerRR(1, 1) #end
        #break
      #case(82) // 'R'
        WRC_RotateYZLayerRR(3, (CCW ? RF : RRF))
        #if(Double) WRC_RotateYZLayerRR(3, 1) #else
          #if(!CCW) WRC_RotateYZLayerRR(3, 1) WRC_RotateYZLayerRR(3, 1) #end
        #end
        #break
      #case(114) // 'r'
        WRC_RotateYZLayerRR(2, (CCW ? RF : RRF))
        #if(Double) WRC_RotateYZLayerRR(2, 1) #else
          #if(!CCW) WRC_RotateYZLayerRR(2, 1) WRC_RotateYZLayerRR(2, 1) #end
        #end
        #break
      #case(66) // 'B'
        WRC_RotateXYLayerRR(3, (CCW ? RF : RRF))
        #if(Double) WRC_RotateXYLayerRR(3, 1) #else
          #if(!CCW) WRC_RotateXYLayerRR(3, 1) WRC_RotateXYLayerRR(3, 1) #end
        #end
        #break
      #case(98) // 'b'
        WRC_RotateXYLayerRR(2, (CCW ? RF : RRF))
        #if(Double) WRC_RotateXYLayerRR(2, 1) #else
          #if(!CCW) WRC_RotateXYLayerRR(2, 1) WRC_RotateXYLayerRR(2, 1) #end
        #end
        #break
      #case(68) // 'D'
        WRC_RotateXZLayerRR(0, (CCW ? RF : RRF))
        #if(Double) WRC_RotateXZLayerRR(0, 1) #else
          #if(!CCW) WRC_RotateXZLayerRR(0, 1) WRC_RotateXZLayerRR(0, 1) #end
        #end
        #break
      #case(100) // 'd'
        WRC_RotateXZLayerRR(1, (CCW ? RF : RRF))
        #if(Double) WRC_RotateXZLayerRR(1, 1) #else
          #if(!CCW) WRC_RotateXZLayerRR(1, 1) WRC_RotateXZLayerRR(1, 1) #end
        #end
        #break
    #end

    #local Ind = Ind+1;
  #end

  union
  { #local XInd = 0;
    #while(XInd < 4)
      #local YInd = 0;
      #while(YInd < 4)
        #local ZInd = 0;
        #while(ZInd < 4)
          #ifdef(WRC_RRData[XInd][YInd][ZInd])
            object { WRC_RRData[XInd][YInd][ZInd] }
          #end
          #local ZInd = ZInd+1;
        #end
        #local YInd = YInd+1;
      #end
      #local XInd = XInd+1;
    #end

    pigment { rgb 0 }
    finish { specular .5 roughness .1 }
  }
#end
