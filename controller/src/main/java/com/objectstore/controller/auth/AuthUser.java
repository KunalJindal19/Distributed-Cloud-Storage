package com.objectstore.controller.auth;

/** The account a public API request was made with. The admin may see and change every object. */
public record AuthUser(String userId, String email, boolean admin) {

    /** The owner to limit a listing to: none for the admin, the account itself for everyone else. */
    public String listingOwner() {
        return admin ? null : userId;
    }

    /** True if this account may see and change an object owned by {@code ownerId}. */
    public boolean mayAccess(String ownerId) {
        return admin || userId.equals(ownerId);
    }
}
