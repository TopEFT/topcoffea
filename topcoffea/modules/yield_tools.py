# Tools for looking at yields and comparing dictionaries

class YieldTools():

    # Get a subset of the elements from a list of strings given a whitelist and/or blacklist of substrings
    def filter_lst_of_strs(self,in_lst,substr_whitelist=[],substr_blacklist=[]):

        # Check all elements are strings
        if not (all(isinstance(x,str) for x in in_lst) and all(isinstance(x,str) for x in substr_whitelist) and all(isinstance(x,str) for x in substr_blacklist)):
            raise Exception("Error: This function only filters lists of strings, one of the elements in one of the input lists is not a str.")
        for elem in substr_whitelist:
            if elem in substr_blacklist:
                raise Exception(f"Error: Cannot whitelist and blacklist the same element (\"{elem}\").")

        # Append to the return list
        out_lst = []
        for element in in_lst:
            blacklisted = False
            whitelisted = True
            for substr in substr_blacklist:
                if substr in element:
                    # If any of the substrings are in the element, blacklist it
                    blacklisted = True
            for substr in substr_whitelist:
                if substr not in element:
                    # If any of the substrings are NOT in the element, do not whitelist it
                    whitelisted = False
            if whitelisted and not blacklisted:
                out_lst.append(element)

        return out_lst



    # Takes two dictionaries, returns the list of lists [common keys, keys unique to d1, keys unique to d2]
    def get_common_keys(self,dict1,dict2):

        common_lst = []
        unique_1_lst = []
        unique_2_lst = []

        # Find common keys, and keys unique to d1
        for k1 in dict1.keys():
            if k1 in dict2.keys():
                common_lst.append(k1)
            else:
                unique_1_lst.append(k1)

        # Find keys unique to d2
        for k2 in dict2.keys():
            if k2 not in common_lst:
                unique_2_lst.append(k2)

        return [common_lst,unique_1_lst,unique_2_lst]



    # Get percent difference
    def get_pdiff(self,a,b,in_percent=False):
        #p = (float(a)-float(b))/((float(a)+float(b))/2)
        if ((a is None) or (b is None)):
            p = None
        elif b == 0:
            p = None
        else:
            p = (float(a)-float(b))/float(b)
            if in_percent:
                p = p*100.0
        return p



    # Get the difference between values in nested dictionary, currently can get either percent diff, or absolute diff
    # Returns a dictionary in the same format (currently does not propagate errors, just returns None)
    #   dict = {
    #       k : {
    #           subk : (val,err)
    #       }
    #   }
    def get_diff_between_nested_dicts(self,dict1,dict2,difftype,inpercent=False):

        # Get list of keys common to both dictionaries
        common_keys, d1_keys, d2_keys = self.get_common_keys(dict1,dict2)
        if len(d1_keys+d2_keys) > 0:
            print(f"\nWARNING, keys {d1_keys+d2_keys} are not in both dictionaries.")

        ret_dict = {}
        for k in common_keys:

            ret_dict[k] = {}

            # Get list of sub keys common to both sub dictionaries
            common_subkeys, d1_subkeys, d2_subkeys = self.get_common_keys(dict1[k],dict2[k])
            if len(d1_subkeys+d2_subkeys) > 0:
                print(f"\tWARNING, sub keys {d1_subkeys+d2_subkeys} are not in both dictionaries.")

            for subk in common_subkeys:
                v1,e1 = dict1[k][subk]
                v2,e1 = dict2[k][subk]
                if difftype == "percent_diff":
                    ret_diff = self.get_pdiff(v1,v2,in_percent=inpercent)
                elif difftype == "absolute_diff":
                    ret_diff = v1 - v2
                else:
                    raise Exception(f"Unknown diff type: {difftype}. Exiting...")

                ret_dict[k][subk] = (ret_diff,None)

        return ret_dict


    # Takes yield dicts and prints it
    # Note:
    #   - This function also now optionally takes a tolerance value
    #   - Checks if the differences are larger than that value
    #   - Returns False if any of the values are too large
    #   - Should a different function handle this stuff?
    def print_yld_dicts(self,ylds_dict,tag,show_errs=False,tolerance=None):
        ret = True
        print(f"\n--- {tag} ---\n")
        for proc in ylds_dict.keys():
            print(proc)
            for cat in ylds_dict[proc].keys():
                print(f"    {cat}")
                val , err = ylds_dict[proc][cat]

                # We don't want to check if the val is small
                if tolerance is None:
                    if show_errs:
                        #print(f"\t{val} +- {err}")
                        print(f"\t{val} +- {err} -> {err/val}")
                    else:
                        print(f"\t{val}")

                # We want to check if the val is small
                else:
                    if (val is None) or (abs(val) < abs(tolerance)):
                        print(f"\t{val}")
                    else:
                        print(f"\t{val} -> NOTE: This is larger than tolerance ({tolerance})!")
                        ret = False
        return ret
