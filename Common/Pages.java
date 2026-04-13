package Common;

import java.time.Instant;

public interface Pages {
    public int getPageAddress();
    public void SetModified(boolean modified);
    public boolean getModified();
    public Instant getLastUsed();
    public void updateLastUsed();

}
