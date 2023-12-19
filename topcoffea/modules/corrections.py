import awkward as ak
import dask_awkward as dak
import correctionlib

from topcoffea.modules.paths import topcoffea_path

# Evaluate btag method 1a weight for a single WP (https://twiki.cern.ch/twiki/bin/viewauth/CMS/BTagSFMethods)
#   - Takes as input a given array of eff and sf and a mask for whether or not the events pass a tag
#   - Returns P(DATA)/P(MC)
#   - Where P(MC) = Product over tagged (eff) * Product over not tagged (1-eff)
#   - Where P(DATA) = Product over tagged (eff*sf) * Product over not tagged (1-eff*sf)
def get_method1a_wgt_singlewp(eff,sf,passes_tag):
    p_mc = ak.prod(eff[passes_tag],axis=-1) * ak.prod(1-eff[~passes_tag],axis=-1)
    p_data = ak.prod(eff[passes_tag]*sf[passes_tag],axis=-1) * ak.prod(1-eff[~passes_tag]*sf[~passes_tag],axis=-1)
    wgt = p_data/p_mc
    return wgt

# Evaluate btag sf from central correctionlib json
def btag_sf_eval(jet_collection,wp,year,method,syst):

    # Get the right sf json for the given year
    if year == "2016APV":
        fname = topcoffea_path("data/btag_sf_correctionlib/2016preVFP_UL_btagging.json")
    elif year == "2016":
        fname = topcoffea_path("data/btag_sf_correctionlib/2016postVFP_UL_btagging.json")
    elif year == "2017":
        fname = topcoffea_path("data/btag_sf_correctionlib/2017_UL_btagging.json")
    elif year == "2018":
        fname = topcoffea_path("data/btag_sf_correctionlib/2018_UL_btagging.json")
    else:
        raise Exception(f"Not a known year: {year}")

    abseta = abs(jet_collection.eta)
    pt = jet_collection.pt
    flav = jet_collection.hadronFlavour

    # For now, cap all pt at 1000 https://cms-talk.web.cern.ch/t/question-about-evaluating-sfs-with-correctionlib/31763
    pt = ak.where(pt>1000.0,1000.0,pt)

    # Evaluate the SF
    ceval = correctionlib.CorrectionSet.from_file(fname)

    sf = dak.map_partitions(
        ceval[method].evaluate,
        syst,
        wp,
        flav,
        abseta,
        pt,
    )

    return sf
