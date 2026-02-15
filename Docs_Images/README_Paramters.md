## File Parsers

CGMF parses each of these parameter sets differently, some of the parsers are robust to changes in whitespace, however, many of them are collumn sensitive. These should be documented due to possible rounding errors during sensitivity scoping. Additionally, this could constrain the acceptable values that can be varied over during the sampling portion of this study.

Rounding errors at very small initial values have been observed in `deformations.dat` & `spinscaling.dat` so far, this effect has not yet been observed in kcksyst.dat` but is theoretically possible under certain conditions.

In terms of parser stability, some are fundamentally robust while some may cause CGMF to fail internally. The parsers are grouped as follows:

 ### Robust Parsers:
 
 - `gstrength_gdr_params.dat`
 - `tkemodel.dat`
 - `yamodel.dat`
 - `rta.dat`
 - `spinscaling.dat`
 
 ### Non - Robust Parsers:
 
 - `kcksyst.dat`
 - `deformations.dat`


`kcksyst.dat` has been tested with all scaling factors set to 10 and this runs. Above this things get extremely chaotic. -10 has also been tested, negative scaling factors are not recommended. If we need access to higher scaling factors, we may need to re-write the CGMF parsers

`deformations.dat` is tested and is stable to physical beta2 values. i.e `-1.0 < beta2 < 1.0`. There will however be some loss in precision for very small beta2 values.

## YAmodel and TKEmodel Coupling.

Parameterisation of YAmodel and TKEmodel are coupled, due to the enforced condition that `TKE < Q_{fission}`. Small changes, mostly in the YAmodel paramters can cause unphysical fragment distributions to be sampled from with very small Q values. CGMF can then silently fail by repeatedly calling `Yields::sampleTKE` within `FissionFragments::sampleFissionFragments`. It is unclear whether during the sampling phase that yield curves should be constrained by data as a pre-run rejection criteria or whether the post-run rejection criteria will effectively address this. The CGMF fork will likely be altered to make sure this failure mode exits gracefully. 
