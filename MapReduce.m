BeginPackage["MapReduce`"]

MapReduce::usage="";

Begin["Private`"];

Options[MapReduce]=
    {"Map" -> Identity,
     "Reduce" -> Identity,
     "Combine" -> (Apply[Flatten[List[##],{1}]&,#]&)
    };

MapReduce[OptionsPattern[]] := 
 Function[records, 
  Map[OptionValue["Reduce"][#[[1, 1]], #[[All, 2]]] &, 
   GatherBy[
    Join @@ ParallelMap[
      Function[r, 
       Map[#[[1, 1]] -> OptionValue["Combine"] @@ (#[[All, 2]]) &, 
        GatherBy[Map[OptionValue["Map"], r], First]]], records], 
    First]]]


End[];
EndPackage[];
