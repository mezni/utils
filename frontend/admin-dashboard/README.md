# AdminLTE-Style Dashboard

This dashboard has been transformed to match the AdminLTE design framework, providing a professional and modern interface for managing EV charging stations and partners.

## Features

### 🎨 Design System
- **AdminLTE-inspired styling** with clean, professional appearance
- **Dark sidebar navigation** with light content area
- **Responsive design** that works on all devices
- **Consistent color palette** with proper contrast ratios
- **Modern typography** using Inter font

### 📱 User Interface Components
- **Header** with search, notifications, and user menu
- **Collapsible sidebar** with navigation items
- **Card-based layout** for content organization
- **Data tables** with pagination and sorting
- **Forms** with proper validation and error handling
- **Toast notifications** for user feedback
- **Modal drawers** for entity creation/editing

### 🔧 Technical Features
- **TypeScript** for type safety
- **Tailwind CSS** for utility-first styling
- **Lucide React** for consistent iconography
- **Mock API data** for development
- **Responsive breakpoints** for mobile/tablet/desktop
- **Accessibility features** with proper ARIA labels

## Component Structure

### Layout Components
- `AppLayout` - Main layout with sidebar and header
- `Header` - Top navigation with search and user menu
- `Sidebar` - Collapsible navigation sidebar

### UI Components
- `Button` - Various button variants (primary, secondary, success, danger, etc.)
- `Card` - Flexible card component with header/body/footer sections
- `Badge` - Status and category badges
- `DataTable` - Sortable data tables with pagination
- `CommandBar` - Action bars for page-level operations
- `SideDrawer` - Modal drawers for forms
- `EntityForm` - Reusable form component
- `Toast` - Notification system

### Page Components
- `PartnersPage` - Partner management interface
- `StationsPage` - Station management interface

## Design Tokens

### Colors
- **Primary**: Blue (#0ea5e9) - for primary actions and highlights
- **Success**: Green (#22c55e) - for positive states
- **Warning**: Yellow (#eab308) - for cautionary states
- **Danger**: Red (#ef4444) - for destructive actions
- **Gray Scale**: 50-900 for text and backgrounds

### Spacing
- **8px grid system** for consistent spacing
- **Container max-width**: 1280px for desktop
- **Responsive padding**: 16px mobile, 24px desktop

### Typography
- **Font**: Inter (system font stack)
- **Scale**: 12px to 32px with consistent line heights
- **Weights**: 400 (regular), 500 (medium), 600 (semibold), 700 (bold)

## Getting Started

### Prerequisites
- Node.js 18+
- pnpm (or npm/yarn)

### Installation
```bash
pnpm install
```

### Development
```bash
pnpm dev
```

### Build
```bash
pnpm build
```

## API Integration

The dashboard currently uses mock API data. Replace the mock implementations in:
- `src/api/partners.ts`
- `src/api/stations.ts`

With actual API calls to your backend services.

## Browser Support

- Chrome 90+
- Firefox 88+
- Safari 14+
- Edge 90+

## Accessibility

The dashboard follows WCAG 2.1 guidelines:
- **Keyboard navigation** support
- **Screen reader** compatibility
- **Color contrast** ratios meet WCAG AA standards
- **Focus indicators** for interactive elements
- **Semantic HTML** structure

## Customization

### Styling
- Modify `tailwind.config.js` for color and typography updates
- Update `src/index.css` for custom component styles
- Use the design tokens in `src/components/ui/` for consistency

### Components
- Extend existing components in `src/components/ui/`
- Create new components following the established patterns
- Use the `Button`, `Card`, and `Badge` components as base templates

## Performance

- **Lazy loading** of components
- **Optimized bundle** with tree shaking
- **Image optimization** with responsive attributes
- **Minimal CSS** with Tailwind utility classes

## Contributing

1. Follow the established component patterns
2. Use TypeScript for type safety
3. Ensure accessibility compliance
4. Test on multiple screen sizes
5. Update documentation for new features

## License

This project is part of the BorneMap EV charging platform.