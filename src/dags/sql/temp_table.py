from typing import Final

# Add derived columns:
# yet defined in Amsterdam schema, so needs to be added.
CREATE_BAG_PND: Final = """


   drop table if exists tmp_bag_pand;
   create temp table tmp_bag_pand as
   -- create temp table bag pand
   select
    *
   from
      public.bag_azure_panden bap --foreign datawrapper?
   ;
   --
   CREATE index tmp_tmp_bag_pand_gindx on tmp_bag_pand USING GIST (geometrie);
   --
   vacuum analyze tmp_bag_pand;
    
    

"""

