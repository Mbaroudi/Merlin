--
-- Copyright (c) 2015 EPAM Systems, Inc. All rights reserved.
--
-- Redistribution and use in source and binary forms, with or without
-- modification, are permitted provided that the following conditions are met:
--
-- Redistributions of source code must retain the above copyright notice, this
-- list of conditions and the following disclaimer.
-- Redistributions in binary form must reproduce the above copyright notice, this
-- list of conditions and the following disclaimer in the documentation and/or
-- other materials provided with the distribution.
-- Neither the name of the EPAM Systems, Inc. nor the names of its contributors
-- may be used to endorse or promote products derived from this software without
-- specific prior written permission.
--
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
-- ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
-- WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
-- DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
-- FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
-- DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
-- SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
-- CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
-- OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
-- OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
--
-- See the NOTICE file and the LICENSE file distributed with this work
-- for additional information regarding copyright ownership and licensing.
--


active_snapshot = LOAD '$active_snapshot' USING PigStorage('|') AS (id:int,
                                                                    description:chararray,
                                                                    action:chararray,
                                                                    effective_date:chararray,
                                                                    end_date:chararray);
scd_updates = LOAD '$data_updates' USING PigStorage('|') AS (id:int,
                                                              description:chararray,
                                                              action:chararray);

--join updates with active dims.
-- do full join as far as data can contain new dims, updates for active dims, active dims without updates
grouped = join active_snapshot by (id) full, scd_updates by (id);

--in grouped we can have new dims, changes for active dims or duplicates for already existing dim\
--split  updated dims and dims which were not changed
split grouped
    into
    historical_dims   if active_snapshot::end_date is not null and  active_snapshot::end_date != '99991231',
    new_dims          if scd_updates::id           is not null and  active_snapshot::id is null,
    deleted_dims      if active_snapshot::id       is null     and  active_snapshot::id is not null and active_snapshot::end_date == '99991231',
    dims_with_updates if scd_updates::id           is not null and  active_snapshot::id is not null and active_snapshot::end_date == '99991231';

--here we can have the same duplicates in updates and active
split dims_with_updates into
    was_changed        if scd_updates::id          != active_snapshot::id
                       or scd_updates::description != active_snapshot::description
                       or scd_updates::action      != active_snapshot::action
    , was_not_changed  if  scd_updates::id  == active_snapshot::id
                       and (( scd_updates::description  is null and active_snapshot::description is null) or scd_updates::description == active_snapshot::description)
                       and (( scd_updates::action       is null and active_snapshot::action is null)      or scd_updates::action      == active_snapshot::action);


dims_to_open  = union onschema was_changed, new_dims;
dims_to_close = union onschema was_changed, deleted_dims;
active_dims   = union onschema historical_dims, was_not_changed ;

--close outdated / deleted dimensions
closed_dims_flatten= foreach dims_to_close{
 generate
      active_snapshot::id as id:int,
      active_snapshot::description as description:chararray,
      active_snapshot::action as action:chararray,
      active_snapshot::effective_date as effective_date:chararray,
      $date as end_date:chararray;
   };

--open new and updated dimensions
new_dims_flatten= foreach dims_to_open{
generate
      scd_updates::id as id:int,
      scd_updates::description as description:chararray,
      scd_updates::action as action:chararray,
      $date as effective_date:chararray,
      '99991231' as end_date:chararray;
   };


active_dims_flatten = foreach active_dims{
generate
    active_snapshot::id as id:int,
      active_snapshot::description as description:chararray,
      active_snapshot::action as action:chararray,
      active_snapshot::effective_date as effective_date:chararray,
      active_snapshot::end_date as end_date:chararray;
}

all_dims = union onschema active_dims_flatten, new_dims_flatten, closed_dims_flatten;
final = FILTER all_dims BY id is not null;

STORE final INTO '$output' using PigStorage('|');