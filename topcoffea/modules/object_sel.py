# Tools for object selection

def is_tight_jet(pt, eta, jet_id, pt_cut, eta_cut, id_cut):
    mask = ((pt>pt_cut) & (abs(eta)<eta_cut) & (jet_id>id_cut))
    return mask
