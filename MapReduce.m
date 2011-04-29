BeginPackage["MapReduce`"]

MapReduce::usage="";
BinRules::usage="";
BinPartition::usage="";

Begin["Private`"];

LaunchKernels[];

BinRules[records_, f_: List] := #[[1, 1]] -> f @@ #[[All, 2]] & /@ 
    GatherBy[records, First]
    
BinPartition[list_, binsize_] := 
    With[{p = Ceiling[Length@list/binsize]}, Partition[list, p, p, 1, {}]]

ClearAll[MapReduce];
    
Options[MapReduce] = 
    {"Mapper" -> Automatic, "MapOutputDepth" -> 0,
     "Reducer" -> Automatic, "Combiner" -> Automatic, 
     "MapperNodes" -> Automatic, "ReducerNodes" -> Automatic, 
     "PrintMap" -> (Null &), "PrintCombine" -> (Null &), 
     "PrintShufflerInput" -> (Null &), "PrintShufflerOutput" -> (Null &), 
     "PrintReduce" -> (Null &)};

MapReduce[OptionsPattern[]] := 
    With[{maparg = OptionValue["Mapper"], 
	  outputdepth = OptionValue["MapOutputDepth"],
	  combinearg = OptionValue["Combiner"], 
	  reducearg = OptionValue["Reducer"],
	  $PrintMap = OptionValue["PrintMap"],
	  $PrintCombine = OptionValue["PrintCombine"], 
	  $PrintShufflerInput = OptionValue["PrintShufflerInput"], 
	  $PrintShufflerOutput = OptionValue["PrintShufflerOutput"], 
	  $PrintReduce = OptionValue["PrintReduce"]}, 
	 Block[{map, reduce, combine, mappers, reducers}, 
	       Switch[maparg, Automatic, map = #1 -> #2 &, {_, _}, 
		      map = First[maparg][#1] -> Last[maparg][#2] &, {_}, 
		      map = #1 -> Last[maparg][#2] &,
		      _, map = maparg];
	       Switch[combinearg, Automatic, 
		      combine = (#1 -> Apply[Flatten[List[##], {1}] &, #2]) &, {_, _}, 
		      combine = First[combinearg][#1] -> Last[combinearg][#2] &, {_}, 
		      combine = #1 -> Last[combinearg][#2] &,
		      _, combine = combinearg];
	       Switch[reducearg, Automatic, reduce = #1 -> #2 &, {_, _}, 
		      reduce = First[reducearg][#1] -> Last[reducearg][#2] &, {_}, 
		      reduce = #1 -> Last[reducearg][#2] &,
		      _, reduce = reducearg];
	       mappers = OptionValue["MapperNodes"] /. Automatic :> Length@Kernels[];
	       reducers = OptionValue["ReducerNodes"] /. Automatic :> Length@Kernels[];
	       With[{m = map, od=outputdepth, r = reduce, c = combine, ms = mappers, rs = reducers}, 
		    Function[records, 
			     SortBy[Join @@ 
				    ParallelMap[($PrintReduce@#; #) &[
					r @@@ #] &, ($PrintShufflerOutput@#; #) &@
						BinPartition[($PrintShufflerInput@#; #) &@
							     BinRules[
								 Join @@ 
								 ParallelMap[
								     Function[
									 recordnode, ($PrintCombine@#; #) &[
									     c @@@ BinRules[($PrintMap@#; #) &[
			      	DeleteCases[Nest[Flatten[#[[All, 2]]] &,
				     m @@@ recordnode,
				     od],_[_,$Failed]]]]]], BinPartition[records, ms]], 
								 Join], rs]], First]
			    ]
		   ]
	      ]
	]


End[];
EndPackage[];

DistributeDefinitions["MapReduce`"];